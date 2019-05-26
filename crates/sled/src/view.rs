use super::*;

use std::{collections::BTreeSet, ops::Bound};

#[derive(Clone)]
pub(crate) struct View<'a> {
    pub(crate) pid: PageId,
    pub(crate) lo: &'a IVec,
    pub(crate) hi: &'a IVec,
    pub(crate) is_index: bool,
    pub(crate) next: Option<PageId>,
    pub(crate) ptr: TreePtr<'a>,
    frags: Vec<&'a Frag>,
    base_offset: usize,
    pub(crate) base_data: &'a Data,
    pub(crate) merging_child: Option<PageId>,
    pub(crate) merging: bool,
}

impl<'a> View<'a> {
    pub(crate) fn new(
        pid: PageId,
        ptr: TreePtr<'a>,
        frags: Vec<&'a Frag>,
    ) -> View<'a> {
        let mut view = View {
            pid,
            ptr,
            frags,
            lo: unsafe { std::mem::uninitialized() },
            hi: unsafe { std::mem::uninitialized() },
            is_index: false,
            base_offset: usize::max_value(),
            base_data: unsafe { std::mem::uninitialized() },
            next: None,
            merging_child: None,
            merging: false,
        };

        let mut merge_confirmed = false;

        for (offset, frag) in view.frags.iter().enumerate() {
            match frag {
                Frag::Base(node) => {
                    if view.base_offset == usize::max_value() {
                        // hi and next may be changed via a
                        // parent split if we re-add that frag
                        view.hi = &node.hi;
                        view.next = node.next;
                    }
                    view.lo = &node.lo;
                    view.is_index = node.data.index_ref().is_some();
                    view.base_offset = offset;
                    view.base_data = &node.data;
                    break;
                }
                Frag::ParentMergeIntention(pid) => {
                    if merge_confirmed {
                        merge_confirmed = false;
                    } else {
                        assert!(view.merging_child.is_none());
                        view.merging_child = Some(*pid);
                    }
                }
                Frag::ParentMergeConfirm => {
                    assert!(!merge_confirmed);
                    merge_confirmed = true;
                }
                Frag::ChildMergeCap => {
                    assert!(!view.merging);
                    assert_eq!(offset, 0);
                    view.merging = true;
                }
                _ => {}
            }
        }

        assert_ne!(
            view.base_offset,
            usize::max_value(),
            "view was never initialized with a base"
        );

        view
    }

    pub(crate) fn contains_upper_bound(&self, bound: &Bound<IVec>) -> bool {
        match bound {
            Bound::Unbounded => self.hi.is_empty(),
            Bound::Included(bound) => self.hi > bound,
            Bound::Excluded(bound) => self.hi >= bound,
        }
    }

    pub(crate) fn contains_lower_bound(&self, bound: &Bound<IVec>) -> bool {
        match bound {
            Bound::Unbounded => self.lo.is_empty(),
            Bound::Included(bound) => self.lo <= bound,
            Bound::Excluded(bound) => self.lo < bound,
        }
    }

    fn keys(&self) -> BTreeSet<&IVec> {
        let mut keys: BTreeSet<&IVec> = self
            .base_data
            .leaf_ref()
            .unwrap()
            .iter()
            .map(|(k, _v)| k)
            .collect();

        for offset in (0..self.base_offset).rev() {
            match self.frags[offset] {
                Frag::Set(k, _) => {
                    keys.insert(k);
                }
                Frag::Del(k) => {
                    keys.remove(k);
                }
                Frag::Merge(k, _) => {
                    keys.insert(k);
                }
                Frag::Base(_) => {
                    panic!(
                        "somehow hit 2 base nodes while \
                         searching for a successor"
                    );
                }
                Frag::ChildMergeCap => {}
                Frag::ParentMergeIntention(_) | Frag::ParentMergeConfirm => {
                    panic!(
                        "somehow hit parent merge \
                         frags while searching for a \
                         successor"
                    )
                }
            }
        }

        keys
    }

    pub(crate) fn successor(
        &self,
        bound: &Bound<IVec>,
        config: &Config,
    ) -> Option<(IVec, IVec)> {
        assert!(!self.is_index);

        // This encoding happens this way because
        // keys cannot be lower than the node's lo key.
        let predecessor_key = match bound {
            Bound::Unbounded => prefix_encode(self.lo, self.lo),
            Bound::Included(b) => {
                let max = std::cmp::max(b, self.lo);
                prefix_encode(self.lo, max)
            }
            Bound::Excluded(b) => {
                let max = std::cmp::max(b, self.lo);
                prefix_encode(self.lo, max)
            }
        };

        let keys = self.keys();

        let successor_keys = keys.range(predecessor_key..);

        for encoded_key in successor_keys {
            let decoded_key = prefix_decode(self.lo, &encoded_key);

            if let Bound::Excluded(e) = bound {
                if &*e == &decoded_key {
                    // skip this excluded key
                    continue;
                }
            }

            // try to get this key until it works
            if let Some(value) = self.leaf_value_for_key(&decoded_key, config) {
                return Some((IVec::from(decoded_key), value));
            }
        }

        None
    }

    pub(crate) fn predecessor(
        &self,
        bound: &Bound<IVec>,
        config: &Config,
    ) -> Option<(IVec, IVec)> {
        assert!(!self.is_index);

        // This encoding happens this way because
        // the rightmost (unbounded) node has
        // a hi key represented by the empty slice
        let successor_key = match bound {
            Bound::Unbounded => {
                if self.hi.is_empty() {
                    prefix_encode(self.lo, &[255; 1024 * 1024])
                } else {
                    prefix_encode(self.lo, self.hi)
                }
            }
            Bound::Included(b) => {
                let min = std::cmp::min(b, self.hi);
                prefix_encode(self.lo, min)
            }
            Bound::Excluded(b) => {
                let min = std::cmp::min(b, self.hi);
                prefix_encode(self.lo, min)
            }
        };

        let keys = self.keys();

        let predecessor_keys = keys.range(..=successor_key).rev();

        for encoded_key in predecessor_keys {
            let decoded_key = prefix_decode(self.lo, &encoded_key);

            if let Bound::Excluded(e) = bound {
                if &*e == &decoded_key {
                    // skip this excluded key
                    continue;
                }
            }

            // try to get this key until it works
            if let Some(value) = self.leaf_value_for_key(&decoded_key, config) {
                return Some((IVec::from(decoded_key), value));
            }
        }

        None
    }

    pub(crate) fn is_free(&self) -> bool {
        self.frags.is_empty()
    }

    pub(crate) fn leaf_value_for_key(
        &self,
        key: &[u8],
        config: &Config,
    ) -> Option<IVec> {
        assert!(!self.is_index);

        let mut merge_base = None;
        let mut merges = vec![];

        for frag in self.frags[..self.base_offset + 1].iter() {
            match frag {
                Frag::Set(k, val) if self.key_eq(k, key) => {
                    if merges.is_empty() {
                        return Some(val.clone());
                    } else {
                        merge_base = Some(val);
                        break;
                    }
                }
                Frag::Del(k) if self.key_eq(k, key) => return None,
                Frag::Merge(k, val) if self.key_eq(k, key) => merges.push(val),
                Frag::Base(node) => {
                    let data = &node.data;
                    let items =
                        data.leaf_ref().expect("last_node should be a leaf");
                    let search = items
                        .binary_search_by(|&(ref k, ref _v)| {
                            prefix_cmp_encoded(k, key.as_ref(), &node.lo)
                        })
                        .ok();

                    let val = search.map(|idx| &items[idx].1);
                    if merges.is_empty() {
                        return val.cloned();
                    } else {
                        merge_base = val;
                    }
                }
                _ => {}
            }
        }

        if merges.is_empty() {
            None
        } else {
            let merge_fn_ptr = config
                .merge_operator
                .expect("must have a merge operator set");

            unsafe {
                let merge_fn: MergeOperator = std::mem::transmute(merge_fn_ptr);

                let mut ret = merge_fn(
                    key,
                    merge_base.map(|iv| &**iv),
                    &merges.pop().unwrap(),
                );
                                       ;
                for merge in merges.into_iter().rev() {
                    if let Some(v) = ret {
                        ret = merge_fn(key, Some(&*v), merge);
                    } else {
                        ret = merge_fn(key, None, merge);
                    }
                }

                ret.map(IVec::from)
            }
        }
    }

    #[inline]
    fn key_eq(&self, encoded: &[u8], not_encoded: &[u8]) -> bool {
        prefix_cmp_encoded(encoded, not_encoded, self.lo)
            == std::cmp::Ordering::Equal
    }

    pub(crate) fn index_next_node(&self, key: &[u8]) -> PageId {
        assert!(self.is_index);

        for frag in self.frags[..self.base_offset + 1].iter() {
            match frag {
                Frag::Set(..) => unimplemented!(),
                Frag::Del(..) => unimplemented!(),
                Frag::Merge(..) => unimplemented!(),
                Frag::Base(node) => {
                    let data = &node.data;
                    let items =
                        data.index_ref().expect("last_node should be a leaf");
                    let search =
                        binary_search_lub(items, |&(ref k, ref _v)| {
                            prefix_cmp_encoded(k, key.as_ref(), &node.lo)
                        });

                    // This might be none if ord is Less and we're
                    // searching for the empty key
                    let index = search.expect("failed to traverse index");

                    return items[index].1;
                }
                Frag::ParentMergeIntention(_)
                | Frag::ParentMergeConfirm
                | Frag::ChildMergeCap => {
                    // nothing to do for these frags
                }
            }
        }
        panic!("no index found")
    }

    pub(crate) fn should_split(&self, max_sz: u64) -> bool {
        let children = self.base_data.len();
        children > 2
            && self.size_in_bytes() > max_sz
            && self.merging_child.is_none()
            && !self.merging
    }

    pub(crate) fn should_merge(&self, min_sz: u64) -> bool {
        self.size_in_bytes() < min_sz
            && self.merging_child.is_none()
            && !self.merging
    }

    pub(crate) fn can_merge_child(&self) -> bool {
        self.merging_child.is_none() && !self.merging
    }

    pub(crate) fn compact(&self, config: &Config) -> Node {
        let mut lhs = self.frags[self.base_offset].unwrap_base().clone();
        for offset in (0..self.base_offset).rev() {
            let frag = self.frags[offset];
            lhs.apply(frag, config.merge_operator);
        }
        lhs
    }

    pub(crate) fn split(&self, config: &Config) -> (Node, Node) {
        let mut lhs = self.compact(config);
        let rhs = lhs.split();

        lhs.data.drop_gte(&rhs.lo, &lhs.lo);
        lhs.hi = rhs.lo.clone();

        // intentionally make this the end to make
        // any issues pop out with setting it
        // correctly after the split.
        lhs.next = None;

        (lhs, rhs)
    }

    #[inline]
    pub(crate) fn size_in_bytes(&self) -> u64 {
        // TODO needs to better account for
        // sizes that don't actually fall under
        // a merge threshold once we support one.
        self.frags[..self.base_offset + 1]
            .iter()
            .map(|f| f.size_in_bytes())
            .sum()
    }
}
