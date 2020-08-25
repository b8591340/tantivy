use crate::query::Query;
use crate::schema::Field;
use crate::{DocAddress, Searcher};
use std::ptr;

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

pub struct HighlightRangesGenerator;

impl HighlightRangesGenerator {
    pub fn generate(
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
            let positions = searcher.positions(query, field, address);
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
