//! String dictionaries shared by the documents of one batch.
//!
//! A [`Dict`] maps a dense `u32` id to a string. A [`Chunk`] is the
//! allocation a [`FlatVariant`](super::FlatVariant) points into: encoded
//! document bytes plus the dictionary those bytes reference. Every value
//! therefore reaches its dictionary through its own chunk and stays
//! self-contained, which is what lets DBSP hand out `&K` with no ambient
//! context and lets a merge compare keys from two different batches.
//!
//! Ids are assigned in first-reference order, never in content order. Content
//! order is recovered by [`Dict::rank`], a permutation computed once at freeze,
//! so comparing two references from one dictionary is an integer compare.
//! Keeping ids out of content order is also what lets one dictionary be
//! appended onto another without renumbering.

use std::sync::Arc;

use size_of::SizeOf;
use xxhash_rust::xxh64::xxh64;

/// Seeded so that entry order stays reproducible from run to run. Ids are
/// internal, so the seed's value carries no compatibility obligation.
const HASH_SEED: u64 = 0x466c_6174_5661_7221;

/// A frozen dictionary. Immutable and shared; build one with [`DictBuilder`].
#[derive(SizeOf)]
pub struct Dict {
    /// Entry bytes, concatenated.
    bytes: Box<[u8]>,
    /// `ends[i]` is where entry `i` ends in `bytes`; entry 0 starts at 0.
    ends: Box<[u32]>,
    /// `ranks[i]` is entry `i`'s position in content order, so
    /// `ranks[a] < ranks[b]` exactly when `get(a) < get(b)`.
    ranks: Box<[u32]>,
    /// Documents interned against this dictionary before it froze. A batch
    /// builder compares it against the dictionary's live reference count to
    /// decide whether the dictionary has gone sparse.
    origin_docs: u32,
    /// The interned map shapes, concatenated. A shape is
    /// `[arity u32][key_end u32 x arity][key bytes]`: a general map's key area
    /// together with the table that delimits it, which is exactly what a
    /// shaped map no longer carries. Keys keep their encoded form, so a shaped
    /// map's key still reads back as an ordinary value, just from here instead
    /// of from the document, and a key too short to earn a dictionary entry
    /// can sit inline beside referenced ones rather than costing the whole map
    /// its shape.
    shape_keys: Box<[u8]>,
    /// `shape_ends[i]` is where shape `i` ends in `shape_keys`.
    shape_ends: Box<[u32]>,
}

impl Dict {
    #[inline]
    pub(crate) fn get(&self, id: u32) -> &[u8] {
        let id = id as usize;
        let start = if id == 0 {
            0
        } else {
            self.ends[id - 1] as usize
        };
        &self.bytes[start..self.ends[id] as usize]
    }

    /// Position of entry `id` in content order. Comparable only against ranks
    /// from the same dictionary.
    #[inline]
    pub(crate) fn rank(&self, id: u32) -> u32 {
        self.ranks[id as usize]
    }

    pub(crate) fn len(&self) -> usize {
        self.ends.len()
    }

    /// Shape `id`, as laid out above.
    #[inline]
    pub(crate) fn shape(&self, id: u32) -> &[u8] {
        let id = id as usize;
        let start = if id == 0 {
            0
        } else {
            self.shape_ends[id - 1] as usize
        };
        &self.shape_keys[start..self.shape_ends[id] as usize]
    }

    /// How many distinct map shapes this dictionary holds.
    pub fn shape_count(&self) -> usize {
        self.shape_ends.len()
    }

    /// Bytes the dictionary occupies, for telemetry and tests.
    pub fn byte_size(&self) -> usize {
        self.bytes.len() + 8 * self.ends.len() + self.shape_keys.len() + 4 * self.shape_ends.len()
    }

    pub fn origin_docs(&self) -> u32 {
        self.origin_docs
    }

    /// Whether most of the documents that built this dictionary are gone, so
    /// the entries they contributed are dead weight.
    ///
    /// The strong count of the `Arc` is the number of live chunks holding the
    /// dictionary, and a chunk is one batch region's worth of documents, so
    /// this is a liveness estimate that costs one relaxed atomic load.
    ///
    /// This is the test a batch builder applies to decide whether to rewire a
    /// value or share its dictionary. Nothing calls it until that integration
    /// lands; the unit test below pins its behaviour meanwhile.
    pub fn is_sparse(this: &Arc<Dict>, factor: u32) -> bool {
        let live = Arc::strong_count(this) as u64;
        live * u64::from(factor) < u64::from(this.origin_docs)
    }
}

/// Interns strings into a dense id space.
///
/// The index is open-addressed over the builder's own byte arena, so an entry
/// is stored once rather than once here and once in a map key.
pub struct DictBuilder {
    bytes: Vec<u8>,
    ends: Vec<u32>,
    /// Entry id plus one, or 0 for an empty slot. Always a power of two.
    slots: Vec<u32>,
    docs: u32,
    /// Interned shapes, as concatenated key areas.
    shape_keys: Vec<u8>,
    shape_ends: Vec<u32>,
    /// Shape id by key area, so a repeated shape is stored once. Shapes are
    /// few enough that an ordinary map is fine here.
    shape_ids: std::collections::HashMap<Box<[u8]>, u32>,
}

impl Default for DictBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DictBuilder {
    pub fn new() -> Self {
        DictBuilder {
            bytes: Vec::new(),
            ends: Vec::new(),
            slots: vec![0; 64],
            docs: 0,
            shape_keys: Vec::new(),
            shape_ends: Vec::new(),
            shape_ids: std::collections::HashMap::new(),
        }
    }

    /// The bytes of entry `id`.
    #[inline]
    pub fn entry(&self, id: u32) -> &[u8] {
        let id = id as usize;
        let start = if id == 0 {
            0
        } else {
            self.ends[id - 1] as usize
        };
        &self.bytes[start..self.ends[id] as usize]
    }

    /// Id of `s`, interning it if this is its first occurrence.
    pub fn intern(&mut self, s: &[u8]) -> u32 {
        let hash = xxh64(s, HASH_SEED);
        let mask = self.slots.len() - 1;
        let mut probe = hash as usize & mask;
        loop {
            match self.slots[probe] {
                0 => break,
                slot if self.entry(slot - 1) == s => return slot - 1,
                _ => probe = (probe + 1) & mask,
            }
        }

        let id: u32 = self
            .ends
            .len()
            .try_into()
            .expect("no more than 4 billion dictionary entries");
        self.bytes.extend_from_slice(s);
        self.ends.push(
            self.bytes
                .len()
                .try_into()
                .expect("no more than 4 GB of dictionary strings"),
        );
        self.slots[probe] = id + 1;

        // Grow at 3/4 load, which keeps probe chains short.
        if self.ends.len() * 4 >= self.slots.len() * 3 {
            self.grow();
        }
        id
    }

    fn grow(&mut self) {
        let mut slots = vec![0u32; self.slots.len() * 2];
        let mask = slots.len() - 1;
        for id in 0..self.ends.len() as u32 {
            let mut probe = xxh64(self.entry(id), HASH_SEED) as usize & mask;
            while slots[probe] != 0 {
                probe = (probe + 1) & mask;
            }
            slots[probe] = id + 1;
        }
        self.slots = slots;
    }

    /// Id of the shape whose key area is `keys`, interning it if new.
    ///
    /// `keys` must already be the whole shape: arity, the end table, then the
    /// encoded keys in canonical order, with any reference's id belonging to
    /// this builder.
    pub fn intern_shape(&mut self, keys: &[u8]) -> u32 {
        if let Some(&id) = self.shape_ids.get(keys) {
            return id;
        }
        let id: u32 = self
            .shape_ends
            .len()
            .try_into()
            .expect("no more than 4 billion shapes");
        self.shape_keys.extend_from_slice(keys);
        self.shape_ends.push(
            self.shape_keys
                .len()
                .try_into()
                .expect("no more than 4 GB of shapes"),
        );
        self.shape_ids.insert(Box::from(keys), id);
        id
    }

    pub fn shape_count(&self) -> usize {
        self.shape_ends.len()
    }

    /// Record that one more document was interned against this dictionary.
    pub fn count_document(&mut self) {
        self.docs += 1;
    }

    pub fn len(&self) -> usize {
        self.ends.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ends.is_empty()
    }

    /// Bytes the entries occupy, so a caller can cap a dictionary's growth.
    pub fn byte_size(&self) -> usize {
        self.bytes.len() + 8 * self.ends.len()
    }

    pub fn freeze(self) -> Arc<Dict> {
        let n = self.ends.len();
        let mut order: Vec<u32> = (0..n as u32).collect();
        order.sort_unstable_by(|&a, &b| self.entry(a).cmp(self.entry(b)));
        let mut ranks = vec![0u32; n];
        for (rank, &id) in order.iter().enumerate() {
            ranks[id as usize] = rank as u32;
        }
        Arc::new(Dict {
            bytes: self.bytes.into_boxed_slice(),
            ends: self.ends.into_boxed_slice(),
            ranks: ranks.into_boxed_slice(),
            origin_docs: self.docs,
            shape_keys: self.shape_keys.into_boxed_slice(),
            shape_ends: self.shape_ends.into_boxed_slice(),
        })
    }
}

/// One allocation of encoded document bytes and the dictionary they reference.
///
/// A chunk holds one document or a whole batch region's worth. Documents in one
/// chunk share a lifetime, so packing many together trades allocation count
/// against the risk that one survivor pins the rest.
#[derive(SizeOf)]
pub struct Chunk {
    dict: Option<Arc<Dict>>,
    bytes: Box<[u8]>,
}

impl Chunk {
    pub(crate) fn new(bytes: Box<[u8]>, dict: Option<Arc<Dict>>) -> Arc<Chunk> {
        Arc::new(Chunk { dict, bytes })
    }

    #[inline]
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[inline]
    pub(crate) fn dict(&self) -> Option<&Arc<Dict>> {
        self.dict.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interning_is_by_content() {
        let mut b = DictBuilder::new();
        assert_eq!(b.intern(b"alpha"), 0);
        assert_eq!(b.intern(b"beta"), 1);
        assert_eq!(b.intern(b"alpha"), 0);
        assert_eq!(b.len(), 2);
        let d = b.freeze();
        assert_eq!(d.get(0), b"alpha");
        assert_eq!(d.get(1), b"beta");
    }

    #[test]
    fn empty_string_is_an_entry_like_any_other() {
        let mut b = DictBuilder::new();
        assert_eq!(b.intern(b""), 0);
        assert_eq!(b.intern(b"x"), 1);
        assert_eq!(b.intern(b""), 0);
        let d = b.freeze();
        assert_eq!(d.get(0), b"");
        assert_eq!(d.get(1), b"x");
        assert!(d.rank(0) < d.rank(1));
    }

    /// Ranks must order by content even though ids are assigned in
    /// first-reference order, because that is what makes an intra-dictionary
    /// string comparison an integer compare.
    #[test]
    fn ranks_follow_content_order_not_id_order() {
        let mut b = DictBuilder::new();
        for s in ["zulu", "alpha", "mike"] {
            b.intern(s.as_bytes());
        }
        let d = b.freeze();
        assert_eq!((d.rank(0), d.rank(1), d.rank(2)), (2, 0, 1));
        for a in 0..d.len() as u32 {
            for c in 0..d.len() as u32 {
                assert_eq!(
                    d.rank(a).cmp(&d.rank(c)),
                    d.get(a).cmp(d.get(c)),
                    "rank order must match content order for {a} and {c}"
                );
            }
        }
    }

    /// Growth rehashes every entry; ids and lookups must survive it.
    #[test]
    fn growth_preserves_ids() {
        let mut b = DictBuilder::new();
        let words: Vec<String> = (0..5000).map(|i| format!("entry-{i}")).collect();
        for (i, w) in words.iter().enumerate() {
            assert_eq!(b.intern(w.as_bytes()), i as u32);
        }
        for (i, w) in words.iter().enumerate() {
            assert_eq!(b.intern(w.as_bytes()), i as u32, "lookup after growth");
        }
        let d = b.freeze();
        assert_eq!(d.len(), words.len());
        for (i, w) in words.iter().enumerate() {
            assert_eq!(d.get(i as u32), w.as_bytes());
        }
    }

    #[test]
    fn sparseness_tracks_live_chunks() {
        let mut b = DictBuilder::new();
        b.intern(b"x");
        for _ in 0..100 {
            b.count_document();
        }
        let d = b.freeze();
        // One live reference against 100 origin documents.
        assert!(Dict::is_sparse(&d, 4));
        let _held: Vec<Arc<Dict>> = (0..40).map(|_| d.clone()).collect();
        assert!(!Dict::is_sparse(&d, 4));
    }
}
