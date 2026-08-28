use super::*;

impl Neo4rDatabase {
    pub fn create_node_property_index(
        &mut self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.add_index_definition(IndexDefinition::node_property(name, label, property))
    }

    pub fn create_node_property_index_if_not_exists(
        &mut self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.add_index_definition_if_not_exists(IndexDefinition::node_property(
            name, label, property,
        ))
    }

    pub fn create_unique_node_property_constraint(
        &mut self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.add_index_definition(IndexDefinition::unique_node_property(name, label, property))
    }

    pub fn create_unique_node_property_constraint_if_not_exists(
        &mut self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.add_index_definition_if_not_exists(IndexDefinition::unique_node_property(
            name, label, property,
        ))
    }

    pub fn create_vector_index(
        &mut self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
        dimensions: usize,
        metric: impl Into<String>,
    ) -> DatabaseResult<()> {
        if dimensions == 0 {
            return Err(DatabaseError::InvalidConfig(
                "vector index dimensions must be greater than zero".to_string(),
            ));
        }
        self.add_index_definition(IndexDefinition::vector(
            name, label, property, dimensions, metric,
        ))
    }

    pub fn create_vector_index_if_not_exists(
        &mut self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
        dimensions: usize,
        metric: impl Into<String>,
    ) -> DatabaseResult<()> {
        if dimensions == 0 {
            return Err(DatabaseError::InvalidConfig(
                "vector index dimensions must be greater than zero".to_string(),
            ));
        }
        self.add_index_definition_if_not_exists(IndexDefinition::vector(
            name, label, property, dimensions, metric,
        ))
    }

    pub fn drop_index(&mut self, name: &str) -> DatabaseResult<()> {
        let before = self.index_catalog.indexes.len();
        self.index_catalog
            .indexes
            .retain(|index| index.name != name);
        if self.index_catalog.indexes.len() == before {
            return Err(DatabaseError::InvalidConfig(format!(
                "index {name:?} does not exist"
            )));
        }
        self.index_catalog.version += 1;
        self.index_catalog_store.save(&self.index_catalog)?;
        self.vector_indexes
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .remove(name);
        self.save_vector_index_cache()?;
        Ok(())
    }

    pub fn drop_index_if_exists(&mut self, name: &str) -> DatabaseResult<()> {
        if self
            .index_catalog
            .indexes
            .iter()
            .any(|index| index.name == name)
        {
            self.drop_index(name)
        } else {
            Ok(())
        }
    }

    pub fn drop_constraint(&mut self, name: &str) -> DatabaseResult<()> {
        let Some(index) = self
            .index_catalog
            .indexes
            .iter()
            .find(|index| index.name == name)
        else {
            return Err(DatabaseError::InvalidConfig(format!(
                "constraint {name:?} does not exist"
            )));
        };
        if !matches!(index.kind, IndexKind::UniqueNodeProperty) {
            return Err(DatabaseError::InvalidConfig(format!(
                "index {name:?} is not a constraint"
            )));
        }
        self.drop_index(name)
    }

    pub fn drop_constraint_if_exists(&mut self, name: &str) -> DatabaseResult<()> {
        let Some(index) = self
            .index_catalog
            .indexes
            .iter()
            .find(|index| index.name == name)
        else {
            return Ok(());
        };
        if !matches!(index.kind, IndexKind::UniqueNodeProperty) {
            return Err(DatabaseError::InvalidConfig(format!(
                "index {name:?} is not a constraint"
            )));
        }
        self.drop_index(name)
    }

    pub fn list_indexes(&self) -> Vec<IndexDefinition> {
        self.index_catalog.indexes.clone()
    }

    pub fn index_lifecycle_status(&self) -> DatabaseResult<Vec<IndexLifecycleStatus>> {
        let persisted = self
            .index_lifecycle_store
            .load()?
            .into_iter()
            .map(|status| (status.name.clone(), status))
            .collect::<BTreeMap<_, _>>();
        self.index_catalog
            .indexes
            .iter()
            .map(|index| {
                if let Some(status) = persisted.get(&index.name) {
                    return Ok(status.clone());
                }
                let status = IndexLifecycleStatus {
                    name: index.name.clone(),
                    state: "ready".to_string(),
                    backfilled_entries: self.index_backfilled_entries(index)?,
                    failure: String::new(),
                };
                Ok(status)
            })
            .collect()
    }

    pub fn show_indexes(&self) -> Vec<QueryRow> {
        format_index_rows(&self.index_catalog.indexes)
    }

    pub fn show_index(&self, name: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(vec![format_index_row_by_name(
            &self.index_catalog.indexes,
            name,
        )?])
    }

    pub fn show_vector_indexes(&self) -> Vec<QueryRow> {
        format_vector_index_rows(&self.index_catalog.indexes)
    }

    pub fn show_vector_index(&self, name: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(vec![format_vector_index_row_by_name(
            &self.index_catalog.indexes,
            name,
        )?])
    }

    pub fn show_vector_index_status(&self) -> DatabaseResult<Vec<QueryRow>> {
        Ok(format_vector_index_status_rows(
            &self.vector_index_status()?,
        ))
    }

    pub fn show_vector_index_status_by_name(&self, name: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(format_vector_index_status_rows(&[
            self.vector_index_status_by_name(name)?
        ]))
    }

    pub(in crate::database) fn show_index_rows_for_query(
        &self,
        query: &str,
    ) -> DatabaseResult<Option<Vec<QueryRow>>> {
        if let Some(name) = show_vector_index_status_name(query)? {
            Ok(Some(self.show_vector_index_status_by_name(&name)?))
        } else if is_show_vector_index_status_cypher(query) {
            Ok(Some(self.show_vector_index_status()?))
        } else if let Some(name) = show_vector_index_name(query)? {
            Ok(Some(self.show_vector_index(&name)?))
        } else if let Some(name) = show_index_name(query)? {
            Ok(Some(self.show_index(&name)?))
        } else if is_show_vector_indexes_cypher(query) {
            Ok(Some(self.show_vector_indexes()))
        } else if is_show_indexes_cypher(query) {
            Ok(Some(self.show_indexes()))
        } else {
            Ok(None)
        }
    }

    pub fn show_constraints(&self) -> Vec<QueryRow> {
        format_constraint_rows(&self.index_catalog.indexes)
    }

    pub fn show_constraint(&self, name: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(vec![format_constraint_row_by_name(
            &self.index_catalog.indexes,
            name,
        )?])
    }

    pub(in crate::database) fn show_constraint_rows_for_query(
        &self,
        query: &str,
    ) -> DatabaseResult<Option<Vec<QueryRow>>> {
        if let Some(name) = show_constraint_name(query)? {
            Ok(Some(self.show_constraint(&name)?))
        } else if is_show_constraints_cypher(query) {
            Ok(Some(self.show_constraints()))
        } else {
            Ok(None)
        }
    }

    pub fn vector_index_status(&self) -> DatabaseResult<Vec<VectorIndexStatus>> {
        Ok(self
            .vector_indexes
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .status())
    }

    pub fn vector_index_status_by_name(&self, name: &str) -> DatabaseResult<VectorIndexStatus> {
        let statuses = self.vector_index_status()?;
        statuses
            .into_iter()
            .find(|status| status.name == name)
            .ok_or_else(|| {
                DatabaseError::InvalidConfig(format!("vector index {name:?} does not exist"))
            })
    }

    pub(in crate::database) fn index_backfilled_entries(
        &self,
        index: &IndexDefinition,
    ) -> DatabaseResult<usize> {
        Ok(self
            .store
            .nodes()?
            .into_iter()
            .filter(|node| {
                node.labels.iter().any(|label| label == &index.label)
                    && node.properties.contains_key(&index.property)
            })
            .count())
    }

    pub(in crate::database) fn save_index_lifecycle_state(
        &self,
        index: &IndexDefinition,
        state: &str,
        backfilled_entries: usize,
        failure: &str,
    ) -> DatabaseResult<()> {
        self.index_lifecycle_store
            .save_status(&IndexLifecycleStatus {
                name: index.name.clone(),
                state: state.to_string(),
                backfilled_entries,
                failure: failure.to_string(),
            })
    }

    pub fn index_catalog(&self) -> IndexCatalog {
        self.index_catalog.clone()
    }

    pub fn install_index_catalog(&mut self, catalog: IndexCatalog) -> DatabaseResult<()> {
        validate_index_catalog(&catalog)?;
        if catalog.version < self.index_catalog.version {
            return Err(DatabaseError::InvalidConfig(format!(
                "index catalog version must not move backwards from {} to {}",
                self.index_catalog.version, catalog.version
            )));
        }
        if catalog.version == self.index_catalog.version
            && catalog.indexes == self.index_catalog.indexes
        {
            return Ok(());
        }
        self.validate_index_catalog_against_store(&catalog)?;
        let vector_indexes = self.build_vector_indexes_for_catalog(&catalog)?;
        self.index_catalog_store.save(&catalog)?;
        self.index_catalog = catalog;
        *self
            .vector_indexes
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)? = vector_indexes;
        self.save_vector_index_cache()
    }

    pub(in crate::database) fn match_node_ids(
        &self,
        matcher: &NodeMatcher,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<NodeId>> {
        query_match_node_ids(|query| self.query_with_params(query, params), matcher)
    }

    pub(in crate::database) fn match_relationship_ids(
        &self,
        matcher: &RelationshipMatcher,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<RelationshipId>> {
        query_match_relationship_ids(|query| self.query_with_params(query, params), matcher)
    }
}
