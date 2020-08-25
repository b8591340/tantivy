use crate::docset::DocSet;
use crate::query::Query;
use crate::schema::{Field, IndexRecordOption};
use crate::{DocAddress, Searcher};
use std::collections::BTreeSet;
use std::mem::MaybeUninit;
use std::ptr;
use std::slice;

const BUFFER_LEN: usize = 1 << 8;

#[repr(C)]
#[derive(Debug)]
pub struct HighlightRange {
    lower: usize,
    upper: usize,
}

impl HighlightRange {
    #[inline(always)]
    pub fn bounds(&self) -> (usize, usize) {
        (self.lower, self.upper)
    }
}

pub struct HighlightRangesGenerator {
    buffer: [u32; BUFFER_LEN],
}

impl HighlightRangesGenerator {
    #[rustfmt::skip]
    pub fn new() -> Self { Self { buffer: unsafe { MaybeUninit::uninit().assume_init() } } }

    pub fn generate(
        &mut self,
        searcher: &Searcher,
        query: &dyn Query,
        field: Field,
        address: DocAddress,
        text: &str,
        limit: Option<usize>,
    ) -> Vec<HighlightRange> {
        if text.is_empty() {
            Vec::new()
        } else {
            let positions = unsafe { positions(searcher, query, field, address, &mut self.buffer) };
            if positions.is_empty() {
                Vec::new()
            } else {
                let mut ranges = Vec::<HighlightRange>::with_capacity(positions.len());
                let mut token_stream = searcher
                    .index()
                    .tokenizer_for_field(field)
                    .expect("text_field")
                    .token_stream(text.as_ref());
                let (ptr, mut len) = (ranges.as_mut_ptr(), 0);
                while let Some(token) = token_stream
                    .next()
                    .filter(|token| limit.map(|limit| token.offset_to <= limit).unwrap_or(true))
                {
                    if positions.contains(&(token.position as u32)) {
                        let (lower, upper) = (token.offset_from, token.offset_to);
                        unsafe { ptr::write(ptr.add(len), HighlightRange { lower, upper }) }
                        len += 1
                    }
                }
                unsafe { ranges.set_len(len) }
                ranges
            }
        }
    }
}

/// Return the positions of the given `DocAddress`, `Field`, `Query` combination
/// for a `DocAddress` returned by `Query` wherein `Field` appears at least once.
///
/// Requires the text field to be indexed with `IndexRecordOption::WithFreqsAndPositions`.
unsafe fn positions<'p>(
    searcher: &Searcher,
    query: &dyn Query,
    field: Field,
    address: DocAddress,
    buffer: &'p mut [u32; BUFFER_LEN],
) -> &'p [u32] {
    let DocAddress(segment_ord, doc_id) = address;
    let segment_reader = searcher.segment_reader(segment_ord);
    let inverted_index = segment_reader.inverted_index(field);
    let term_dict = inverted_index.terms();
    let mut terminfos = BTreeSet::new();
    query.terminfos(&mut terminfos, term_dict, field);
    let ptr = buffer.as_mut_ptr();
    ptr::write(ptr, 0); // We store `len` at the start.
    for term_info in terminfos.into_iter() {
        let mut postings = inverted_index
            .read_postings_from_terminfo(&term_info, IndexRecordOption::WithFreqsAndPositions);
        if postings.seek(doc_id) == doc_id {
            if let Some(position_reader) = postings.position_reader.as_mut() {
                let len = ptr::read(ptr) as usize;
                let additional = postings.block_cursor.freq(postings.cur) as usize; // `term_freq()` assert in `seek`
                debug_assert!(len + additional <= BUFFER_LEN - 1);
                ptr::write(ptr, (len + additional) as u32);
                let offset = postings.block_cursor.position_offset()
                    + (postings.block_cursor.freqs()[..postings.cur]
                        .iter()
                        .cloned()
                        .sum::<u32>() as u64);
                let output = slice::from_raw_parts_mut(ptr.add(1).add(len), additional);
                position_reader.read(offset, output);
            }
        }
    }
    slice::from_raw_parts(ptr.add(1), ptr::read(ptr) as usize)
}
