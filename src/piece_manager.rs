//! Piece state tracker shared across all peer tasks.
//!
//! Pieces move through these states:
//!   Needed → Claimed (by a single peer task) → Done
//!
//! If a peer task fails it *must* call [`PieceManager::return_piece`] so the
//! piece can be re-claimed by another peer.

use std::collections::HashSet;

/// Lifecycle state of a single piece.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PieceState {
    /// Piece has not been started.
    Needed,
    /// Piece is being downloaded by a peer task.
    Claimed,
    /// Piece has been verified and written to disk.
    Done,
}

/// Tracks piece state across all peer tasks for a single torrent.
pub struct PieceManager {
    piece_count: u32,
    state: Vec<PieceState>,
    pub piece_length: u64,
    pub total_length: u64,
    pub downloaded: u64,
    /// Number of pieces marked Done.
    done_count: u32,
    /// Number of pieces in the `Needed` state (O(1) cache).
    needed_count: u32,
}

impl PieceManager {
    pub fn new(piece_count: u32, piece_length: u64, total_length: u64) -> Self {
        PieceManager {
            piece_count,
            state: vec![PieceState::Needed; piece_count as usize],
            piece_length,
            total_length,
            downloaded: 0,
            done_count: 0,
            needed_count: piece_count,
        }
    }

    pub fn piece_count(&self) -> u32 {
        self.piece_count
    }

    pub fn is_complete(&self) -> bool {
        self.done_count == self.piece_count
    }

    /// Claim the first needed piece that the peer has (simple sequential strategy).
    /// Returns the piece index, or `None` if nothing is available for this peer.
    pub fn claim_piece(&mut self, peer_bitfield: &[bool]) -> Option<u32> {
        for (i, state) in self.state.iter_mut().enumerate() {
            if *state == PieceState::Needed {
                let peer_has = peer_bitfield.get(i).copied().unwrap_or(false);
                if peer_has {
                    *state = PieceState::Claimed;
                    self.needed_count -= 1;
                    return Some(i as u32);
                }
            }
        }
        None
    }

    /// Return a piece back to Needed (e.g. if a peer disconnected mid-download).
    pub fn return_piece(&mut self, index: u32) {
        if let Some(s) = self.state.get_mut(index as usize) {
            if *s == PieceState::Claimed {
                *s = PieceState::Needed;
                self.needed_count += 1;
            }
        }
    }

    /// Mark a piece as successfully verified and written to disk.
    pub fn mark_done(&mut self, index: u32, piece_len: u64) {
        if let Some(s) = self.state.get_mut(index as usize) {
            match *s {
                PieceState::Done => {}
                PieceState::Needed => {
                    *s = PieceState::Done;
                    self.needed_count -= 1;
                    self.done_count += 1;
                    self.downloaded += piece_len;
                }
                PieceState::Claimed => {
                    *s = PieceState::Done;
                    self.done_count += 1;
                    self.downloaded += piece_len;
                }
            }
        }
    }

    /// True if we still need this piece.
    pub fn needs(&self, index: u32) -> bool {
        self.state
            .get(index as usize)
            .map(|s| *s != PieceState::Done)
            .unwrap_or(false)
    }

    /// Length of a specific piece.
    pub fn piece_len(&self, index: u32) -> u64 {
        let last = self.piece_count.saturating_sub(1);
        if index == last {
            let rem = self.total_length % self.piece_length;
            if rem == 0 { self.piece_length } else { rem }
        } else {
            self.piece_length
        }
    }

    /// Count of done pieces.
    pub fn done_count(&self) -> u32 {
        self.done_count
    }

    /// Set of pieces the peer has, parsed from a bitfield.
    pub fn bitfield_to_vec(bitfield: &[u8], piece_count: u32) -> Vec<bool> {
        let mut has = vec![false; piece_count as usize];
        for (byte_i, &byte) in bitfield.iter().enumerate() {
            for bit_i in 0..8 {
                let piece_i = byte_i * 8 + bit_i;
                if piece_i >= piece_count as usize {
                    break;
                }
                // Most-significant bit first.
                has[piece_i] = (byte >> (7 - bit_i)) & 1 == 1;
            }
        }
        has
    }

    /// Build a bitfield of all completed pieces.
    pub fn build_bitfield(&self) -> Vec<u8> {
        let num_bytes = (self.piece_count as usize + 7) / 8;
        let mut bits = vec![0u8; num_bytes];
        for i in 0..self.piece_count as usize {
            if self.state[i] == PieceState::Done {
                bits[i / 8] |= 1 << (7 - (i % 8));
            }
        }
        bits
    }

    pub fn needed_count(&self) -> u32 {
        self.needed_count
    }

    /// Indices of all pieces not yet done (for diagnostics).
    pub fn pending_set(&self) -> HashSet<u32> {
        self.state
            .iter()
            .enumerate()
            .filter(|(_, s)| **s != PieceState::Done)
            .map(|(i, _)| i as u32)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pm(pieces: u32, piece_len: u64, total: u64) -> PieceManager {
        PieceManager::new(pieces, piece_len, total)
    }

    #[test]
    fn new_has_correct_piece_count() {
        let pm = make_pm(5, 512, 2560);
        assert_eq!(pm.piece_count(), 5);
    }

    #[test]
    fn new_is_not_complete() {
        let pm = make_pm(3, 256, 768);
        assert!(!pm.is_complete());
    }

    #[test]
    fn zero_pieces_is_complete() {
        let pm = make_pm(0, 256, 0);
        assert!(pm.is_complete());
    }

    #[test]
    fn claim_piece_returns_index_when_peer_has_it() {
        let mut pm = make_pm(3, 256, 768);
        let bitfield = vec![false, true, false];
        assert_eq!(pm.claim_piece(&bitfield), Some(1));
    }

    #[test]
    fn claim_piece_returns_none_when_peer_has_nothing() {
        let mut pm = make_pm(3, 256, 768);
        let bitfield = vec![false; 3];
        assert_eq!(pm.claim_piece(&bitfield), None);
    }

    #[test]
    fn claim_piece_does_not_double_claim() {
        let mut pm = make_pm(2, 256, 512);
        let bitfield = vec![true, true];
        assert_eq!(pm.claim_piece(&bitfield), Some(0));
        // Piece 0 is now Claimed; next claim should give piece 1.
        assert_eq!(pm.claim_piece(&bitfield), Some(1));
        // No more pieces to claim.
        assert_eq!(pm.claim_piece(&bitfield), None);
    }

    #[test]
    fn return_piece_makes_it_reclaimable() {
        let mut pm = make_pm(2, 256, 512);
        let bf = vec![true, true];
        pm.claim_piece(&bf);
        pm.return_piece(0);
        assert_eq!(pm.claim_piece(&bf), Some(0));
    }

    #[test]
    fn mark_done_increments_done_count() {
        let mut pm = make_pm(3, 256, 768);
        let bf = vec![true; 3];
        pm.claim_piece(&bf);
        pm.mark_done(0, 256);
        assert_eq!(pm.done_count(), 1);
        assert!(!pm.needs(0));
    }

    #[test]
    fn mark_done_updates_downloaded() {
        let mut pm = make_pm(2, 512, 1024);
        let bf = vec![true; 2];
        pm.claim_piece(&bf);
        pm.mark_done(0, 512);
        assert_eq!(pm.downloaded, 512);
    }

    #[test]
    fn mark_done_idempotent() {
        let mut pm = make_pm(2, 512, 1024);
        let bf = vec![true; 2];
        pm.claim_piece(&bf);
        pm.mark_done(0, 512);
        pm.mark_done(0, 512); // second call should not double-count
        assert_eq!(pm.done_count(), 1);
        assert_eq!(pm.downloaded, 512);
    }

    #[test]
    fn mark_done_without_prior_claim() {
        let mut pm = make_pm(3, 256, 768);
        pm.mark_done(2, 256);
        assert_eq!(pm.done_count(), 1);
        assert!(!pm.needs(2));
    }

    #[test]
    fn is_complete_after_all_done() {
        let mut pm = make_pm(2, 512, 1024);
        let bf = vec![true; 2];
        pm.claim_piece(&bf);
        pm.mark_done(0, 512);
        pm.claim_piece(&bf);
        pm.mark_done(1, 512);
        assert!(pm.is_complete());
    }

    #[test]
    fn piece_len_normal_piece() {
        let pm = make_pm(3, 1000, 2500);
        assert_eq!(pm.piece_len(0), 1000);
        assert_eq!(pm.piece_len(1), 1000);
    }

    #[test]
    fn piece_len_last_piece_with_remainder() {
        let pm = make_pm(3, 1000, 2500);
        // Last piece: 2500 % 1000 = 500
        assert_eq!(pm.piece_len(2), 500);
    }

    #[test]
    fn piece_len_last_piece_exact_multiple() {
        let pm = make_pm(3, 1000, 3000);
        // 3000 % 1000 == 0, so last piece = piece_length
        assert_eq!(pm.piece_len(2), 1000);
    }

    #[test]
    fn needed_count_decreases_on_done() {
        let mut pm = make_pm(4, 256, 1024);
        assert_eq!(pm.needed_count(), 4);
        let bf = vec![true; 4];
        pm.claim_piece(&bf);
        pm.mark_done(0, 256);
        // Claimed pieces are still not Done, so needed = 3 (Needed state)
        assert_eq!(pm.needed_count(), 3);
    }

    #[test]
    fn pending_set_contains_undone_pieces() {
        let mut pm = make_pm(3, 256, 768);
        let bf = vec![true; 3];
        pm.claim_piece(&bf);
        pm.mark_done(0, 256);
        let pending = pm.pending_set();
        assert!(!pending.contains(&0));
        assert!(pending.contains(&1));
        assert!(pending.contains(&2));
    }

    #[test]
    fn bitfield_to_vec_parses_correctly() {
        // byte 0b10100000 = pieces 0 and 2 are present, 1 and 3 are not.
        let bitfield = vec![0b10100000u8];
        let has = PieceManager::bitfield_to_vec(&bitfield, 4);
        assert_eq!(has, vec![true, false, true, false]);
    }

    #[test]
    fn bitfield_to_vec_truncates_to_piece_count() {
        // 1 byte = 8 bits; piece_count = 3 should ignore the rest.
        let bitfield = vec![0b11111111u8];
        let has = PieceManager::bitfield_to_vec(&bitfield, 3);
        assert_eq!(has.len(), 3);
        assert!(has.iter().all(|&b| b));
    }

    #[test]
    fn build_bitfield_reflects_done_pieces() {
        let mut pm = make_pm(8, 256, 2048);
        let bf = vec![true; 8];
        pm.claim_piece(&bf); // claims piece 0
        pm.mark_done(0, 256);
        let bits = pm.build_bitfield();
        // Piece 0 done: first byte should have bit 7 set = 0b10000000.
        assert_eq!(bits[0], 0b10000000);
    }

    #[test]
    fn build_bitfield_all_done_all_ones() {
        let mut pm = make_pm(8, 256, 2048);
        for i in 0..8u32 {
            pm.mark_done(i, 256);
        }
        let bits = pm.build_bitfield();
        assert_eq!(bits, vec![0b11111111u8]);
    }
}
