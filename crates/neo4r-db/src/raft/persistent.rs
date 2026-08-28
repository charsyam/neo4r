use super::*;
use std::fs;
use std::io::ErrorKind;
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftPersistentState {
    pub current_term: Term,
    pub voted_for: Option<ServerId>,
}

impl Default for RaftPersistentState {
    fn default() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RaftPersistentStateStore {
    path: PathBuf,
}

impl RaftPersistentStateStore {
    pub fn open(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn load(&self) -> DatabaseResult<RaftPersistentState> {
        match fs::read_to_string(&self.path) {
            Ok(text) => decode_persistent_state(&text),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(RaftPersistentState::default()),
            Err(err) => Err(DatabaseError::InvalidConfig(format!(
                "failed to read raft state {}: {err}",
                self.path.display()
            ))),
        }
    }

    pub fn save(&self, state: &RaftPersistentState) -> DatabaseResult<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                DatabaseError::InvalidConfig(format!(
                    "failed to create raft state dir {}: {err}",
                    parent.display()
                ))
            })?;
        }
        let temp_path = temp_path(&self.path);
        fs::write(&temp_path, encode_persistent_state(state)).map_err(|err| {
            DatabaseError::InvalidConfig(format!(
                "failed to write raft state {}: {err}",
                temp_path.display()
            ))
        })?;
        fs::rename(&temp_path, &self.path).map_err(|err| {
            DatabaseError::InvalidConfig(format!(
                "failed to install raft state {}: {err}",
                self.path.display()
            ))
        })
    }
}

pub fn encode_persistent_state(state: &RaftPersistentState) -> String {
    format!(
        "{RAFT_STATE_MAGIC}\nterm={}\nvoted_for={}\n",
        state.current_term,
        state
            .voted_for
            .map(|server_id| server_id.to_string())
            .unwrap_or_else(|| "-".to_string())
    )
}

pub fn decode_persistent_state(input: &str) -> DatabaseResult<RaftPersistentState> {
    let mut lines = input.lines();
    if lines.next() != Some(RAFT_STATE_MAGIC) {
        return Err(DatabaseError::InvalidConfig(
            "invalid raft state header".to_string(),
        ));
    }
    let term = lines
        .next()
        .and_then(|line| line.strip_prefix("term="))
        .ok_or_else(|| DatabaseError::InvalidConfig("missing raft term".to_string()))?
        .parse::<Term>()
        .map_err(|_| DatabaseError::InvalidConfig("invalid raft term".to_string()))?;
    let voted_for = match lines
        .next()
        .and_then(|line| line.strip_prefix("voted_for="))
        .ok_or_else(|| DatabaseError::InvalidConfig("missing raft vote".to_string()))?
    {
        "-" => None,
        value => Some(
            value
                .parse::<ServerId>()
                .map_err(|_| DatabaseError::InvalidConfig("invalid raft vote".to_string()))?,
        ),
    };
    Ok(RaftPersistentState {
        current_term: term,
        voted_for,
    })
}

pub(super) fn temp_path(path: &PathBuf) -> PathBuf {
    path.with_extension("tmp")
}
