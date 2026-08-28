use super::*;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct SnapshotChunkAssembler {
    metadata: RaftSnapshotMetadata,
    term: Term,
    leader_id: ServerId,
    next_offset: u64,
    chunks: BTreeMap<u64, Vec<u8>>,
    done_offset: Option<u64>,
}

impl SnapshotChunkAssembler {
    pub fn new(first: InstallSnapshotChunk) -> DatabaseResult<Self> {
        let mut assembler = Self {
            metadata: first.request.metadata.clone(),
            term: first.request.term,
            leader_id: first.request.leader_id,
            next_offset: 0,
            chunks: BTreeMap::new(),
            done_offset: None,
        };
        assembler.push(first)?;
        Ok(assembler)
    }

    pub fn push(
        &mut self,
        chunk: InstallSnapshotChunk,
    ) -> DatabaseResult<Option<InstallSnapshotRequest>> {
        self.validate(&chunk)?;
        let payload_len = chunk.request.payload.len() as u64;
        self.chunks.insert(chunk.offset, chunk.request.payload);
        if chunk.done {
            self.done_offset = Some(chunk.offset + payload_len);
        }
        self.try_finish()
    }

    fn validate(&self, chunk: &InstallSnapshotChunk) -> DatabaseResult<()> {
        if chunk.request.term != self.term
            || chunk.request.leader_id != self.leader_id
            || chunk.request.metadata != self.metadata
        {
            return Err(DatabaseError::Replication(
                "snapshot chunk metadata mismatch".to_string(),
            ));
        }
        if chunk.offset > self.next_offset {
            return Err(DatabaseError::Replication(format!(
                "snapshot chunk offset gap: got {}, expected {}",
                chunk.offset, self.next_offset
            )));
        }
        Ok(())
    }

    fn try_finish(&mut self) -> DatabaseResult<Option<InstallSnapshotRequest>> {
        let Some(done_offset) = self.done_offset else {
            self.next_offset = contiguous_len(&self.chunks);
            return Ok(None);
        };
        let mut payload = Vec::new();
        let mut offset = 0;
        while offset < done_offset {
            let Some(chunk) = self.chunks.remove(&offset) else {
                self.next_offset = offset;
                return Ok(None);
            };
            offset += chunk.len() as u64;
            payload.extend_from_slice(&chunk);
        }
        Ok(Some(InstallSnapshotRequest {
            term: self.term,
            leader_id: self.leader_id,
            metadata: self.metadata.clone(),
            payload,
        }))
    }
}

fn contiguous_len(chunks: &BTreeMap<u64, Vec<u8>>) -> u64 {
    let mut offset = 0;
    while let Some(chunk) = chunks.get(&offset) {
        offset += chunk.len() as u64;
    }
    offset
}
