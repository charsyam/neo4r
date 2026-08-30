use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterBootstrapMode {
    JoinExisting,
    RecoverFromData,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterBootstrapShard {
    pub shard_id: ShardId,
    pub commit_index: LogIndex,
    pub snapshot_index: LogIndex,
    pub snapshot_term: Term,
    pub snapshot_checksum: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterBootstrapManifest {
    pub format_version: u64,
    pub mode: ClusterBootstrapMode,
    pub cluster_id: String,
    pub database_id: String,
    pub seed_server_id: ServerId,
    pub shard_count: u64,
    pub routing_version: u64,
    pub metadata_term: u64,
    pub config_epoch: u64,
    pub force_new_cluster_required: bool,
    pub shards: Vec<ClusterBootstrapShard>,
    pub membership: ClusterMembership,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeCatchUpSource {
    pub shard_id: ShardId,
    pub primary_server_id: ServerId,
    pub primary_address: String,
    pub snapshot_required: bool,
    pub start_index: LogIndex,
    pub target_index: LogIndex,
    pub current_match_index: LogIndex,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeCatchUpPlan {
    pub server_id: ServerId,
    pub routing_version: u64,
    pub metadata_term: u64,
    pub sources: Vec<NodeCatchUpSource>,
    pub ready_to_promote: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeCatchUpExecution {
    pub server_id: ServerId,
    pub installed_snapshots: usize,
    pub replayed_entries: usize,
    pub shard_results: Vec<NodeCatchUpShardExecution>,
    pub ready_to_promote: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeCatchUpShardExecution {
    pub shard_id: ShardId,
    pub snapshot_installed: bool,
    pub replay_start_index: LogIndex,
    pub replay_end_index: LogIndex,
    pub replayed_entries: usize,
    pub match_index: LogIndex,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotResumeToken {
    pub shard_id: ShardId,
    pub snapshot_index: LogIndex,
    pub snapshot_term: Term,
    pub next_offset: u64,
    pub completed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapSafetyDecision {
    pub allowed: bool,
    pub mode: ClusterBootstrapMode,
    pub requires_force_new_cluster: bool,
    pub expected_cluster_id: String,
    pub observed_cluster_id: String,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackupBootstrapLink {
    pub backup_manifest_path: PathBuf,
    pub bootstrap_cluster_id: String,
    pub database_id: String,
    pub shard_count: u64,
    pub safe_to_seed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopologyObservation {
    pub joining_nodes: usize,
    pub catching_up_assignments: usize,
    pub caught_up_assignments: usize,
    pub draining_nodes: usize,
    pub recommended_action: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationalSafetyDecision {
    pub allowed: bool,
    pub confirmation_required: bool,
    pub confirmation_token: String,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterChaosCheck {
    pub scenario: String,
    pub passed: bool,
    pub checked_invariant: String,
}

#[derive(Clone, Debug)]
pub(in crate::database) struct ClusterBootstrapManifestStore {
    path: PathBuf,
}

impl ClusterBootstrapManifestStore {
    pub(in crate::database) fn open(data_dir: impl AsRef<Path>) -> DatabaseResult<Self> {
        let cluster_dir = data_dir.as_ref().join("cluster");
        fs::create_dir_all(&cluster_dir).map_err(StorageError::Io)?;
        Ok(Self {
            path: cluster_dir.join("bootstrap-manifest.txt"),
        })
    }

    pub(in crate::database) fn save(
        &self,
        manifest: &ClusterBootstrapManifest,
    ) -> DatabaseResult<()> {
        let tmp_path = self.path.with_extension("txt.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(StorageError::Io)?;
        writeln!(
            file,
            "N4RBOOT1\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            manifest.format_version,
            encode_cluster_bootstrap_mode(manifest.mode),
            sanitize_cluster_text(&manifest.cluster_id),
            sanitize_cluster_text(&manifest.database_id),
            manifest.seed_server_id,
            manifest.shard_count,
            manifest.routing_version,
            manifest.metadata_term,
            manifest.config_epoch,
            manifest.force_new_cluster_required as u8
        )
        .map_err(StorageError::Io)?;
        writeln!(file, "membership_version\t{}", manifest.membership.version)
            .map_err(StorageError::Io)?;
        for node in &manifest.membership.nodes {
            writeln!(
                file,
                "node\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                node.server_id,
                sanitize_cluster_text(&node.address),
                encode_node_membership_state(node.state),
                node.protocol_version,
                node.storage_version,
                node.shard_count,
                sanitize_cluster_text(&node.rejection_reason)
            )
            .map_err(StorageError::Io)?;
        }
        for assignment in &manifest.membership.shard_assignments {
            writeln!(
                file,
                "assignment\t{}\t{}\t{}\t{}",
                assignment.shard_id,
                assignment.server_id,
                encode_shard_assignment_state(assignment.state),
                assignment.match_index
            )
            .map_err(StorageError::Io)?;
        }
        for shard in &manifest.shards {
            writeln!(
                file,
                "shard\t{}\t{}\t{}\t{}\t{}",
                shard.shard_id,
                shard.commit_index,
                shard.snapshot_index,
                shard.snapshot_term,
                shard.snapshot_checksum
            )
            .map_err(StorageError::Io)?;
        }
        file.sync_all().map_err(StorageError::Io)?;
        drop(file);
        fs::rename(&tmp_path, &self.path).map_err(StorageError::Io)?;
        if let Some(parent) = self.path.parent() {
            File::open(parent)
                .map_err(StorageError::Io)?
                .sync_all()
                .map_err(StorageError::Io)?;
        }
        Ok(())
    }

    pub(in crate::database) fn load(&self) -> DatabaseResult<Option<ClusterBootstrapManifest>> {
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(StorageError::Io(err).into()),
        };
        let mut lines = BufReader::new(file).lines();
        let header = lines
            .next()
            .transpose()
            .map_err(StorageError::Io)?
            .ok_or_else(|| {
                StorageError::CorruptStore("missing cluster bootstrap manifest header".to_string())
            })?;
        let parts = header.split('\t').collect::<Vec<_>>();
        if parts.len() != 11 || parts[0] != "N4RBOOT1" {
            return Err(StorageError::CorruptStore(
                "invalid cluster bootstrap manifest header".to_string(),
            )
            .into());
        }
        let mut membership_version = None;
        let mut nodes = Vec::new();
        let mut assignments = Vec::new();
        let mut shards = Vec::new();
        for line in lines {
            let line = line.map_err(StorageError::Io)?;
            if line.is_empty() {
                continue;
            }
            let record = line.split('\t').collect::<Vec<_>>();
            match record.first().copied().unwrap_or_default() {
                "membership_version" if record.len() == 2 => {
                    membership_version =
                        Some(parse_plan_u64(record[1], "bootstrap membership version")?);
                }
                "node" if record.len() == 8 => nodes.push(ClusterNode {
                    server_id: parse_plan_u64(record[1], "bootstrap node server id")?,
                    address: record[2].to_string(),
                    state: decode_node_membership_state(record[3])?,
                    protocol_version: parse_plan_u64(record[4], "bootstrap node protocol")?,
                    storage_version: parse_plan_u64(record[5], "bootstrap node storage")?,
                    shard_count: parse_plan_u64(record[6], "bootstrap node shard count")?,
                    rejection_reason: record[7].to_string(),
                }),
                "assignment" if record.len() == 5 => assignments.push(ClusterShardAssignment {
                    shard_id: parse_plan_u64(record[1], "bootstrap assignment shard")?,
                    server_id: parse_plan_u64(record[2], "bootstrap assignment server")?,
                    state: decode_shard_assignment_state(record[3])?,
                    match_index: parse_plan_u64(record[4], "bootstrap assignment match index")?,
                }),
                "shard" if record.len() == 6 => shards.push(ClusterBootstrapShard {
                    shard_id: parse_plan_u64(record[1], "bootstrap shard id")?,
                    commit_index: parse_plan_u64(record[2], "bootstrap shard commit index")?,
                    snapshot_index: parse_plan_u64(record[3], "bootstrap shard snapshot index")?,
                    snapshot_term: parse_plan_u64(record[4], "bootstrap shard snapshot term")?,
                    snapshot_checksum: parse_plan_u64(record[5], "bootstrap shard checksum")?,
                }),
                _ => {
                    return Err(StorageError::CorruptStore(
                        "invalid cluster bootstrap manifest record".to_string(),
                    )
                    .into());
                }
            }
        }
        Ok(Some(ClusterBootstrapManifest {
            format_version: parse_plan_u64(parts[1], "bootstrap format version")?,
            mode: decode_cluster_bootstrap_mode(parts[2])?,
            cluster_id: parts[3].to_string(),
            database_id: parts[4].to_string(),
            seed_server_id: parse_plan_u64(parts[5], "bootstrap seed server id")?,
            shard_count: parse_plan_u64(parts[6], "bootstrap shard count")?,
            routing_version: parse_plan_u64(parts[7], "bootstrap routing version")?,
            metadata_term: parse_plan_u64(parts[8], "bootstrap metadata term")?,
            config_epoch: parse_plan_u64(parts[9], "bootstrap config epoch")?,
            force_new_cluster_required: parse_plan_bool(
                parts[10],
                "bootstrap force new cluster flag",
            )?,
            shards,
            membership: ClusterMembership {
                version: membership_version.ok_or_else(|| {
                    StorageError::CorruptStore(
                        "missing cluster bootstrap membership version".to_string(),
                    )
                })?,
                nodes,
                shard_assignments: assignments,
            },
        }))
    }
}
