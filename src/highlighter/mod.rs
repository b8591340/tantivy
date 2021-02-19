use crate::query::Query;
use crate::schema::Field;
use crate::{DocAddress, Searcher};
use std::ptr;

#[repr(C)]
#[derive(Debug)]
pub struct TextRange {
    highlight: bool,
    lower: usize,
    upper: usize,
}

#[rustfmt::skip]
impl TextRange {
    #[inline(always)]
    fn whole(upper: usize) -> Vec<Self> {
        vec! { Self { highlight: false, lower: 0, upper } }
    }

    #[inline(always)]
    fn write(highlight: bool, lower: usize, upper: usize, ptr: *mut TextRange, len: &mut usize) {
        unsafe { ptr::write(ptr.add(*len), Self { highlight, lower, upper }) }
        *len += 1
    }
}

pub struct TextRangesGenerator;

impl TextRangesGenerator {
    pub fn generate(
        searcher: &Searcher,
        query: &dyn Query,
        field: Field,
        address: DocAddress,
        text: &str,
    ) -> crate::Result<Vec<TextRange>> {
        if text.is_empty() {
            Ok(Vec::new())
        } else {
            let upper = text.len();
            let positions = searcher.positions(query, field, address)?;
            if positions.is_empty() {
                Ok(TextRange::whole(upper))
            } else {
                let capacity = positions.len() + positions.len() + 1;
                let mut ranges = Vec::<TextRange>::with_capacity(capacity);
                let mut token_stream = searcher
                    .index()
                    .tokenizer_for_field(field)
                    .expect("text_field")
                    .token_stream(text);
                let (ptr, mut len, mut lower) = (ranges.as_mut_ptr(), 0, 0);
                while let Some(token) = token_stream.next() {
                    if positions.contains(&(token.position as u32)) {
                        let (token_lower, token_upper) = (token.offset_from, token.offset_to);
                        if token_lower > lower {
                            TextRange::write(false, lower, token_lower, ptr, &mut len);
                        }
                        TextRange::write(true, token_lower, token_upper, ptr, &mut len);
                        lower = token_upper
                    }
                }
                debug_assert!(lower <= upper);
                if lower != upper {
                    TextRange::write(false, lower, upper, ptr, &mut len);
                }
                debug_assert!(len <= capacity);
                unsafe { ranges.set_len(len) }
                Ok(ranges)
            }
        }
    }
}

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
    ) -> crate::Result<Vec<HighlightRange>> {
        if text.is_empty() {
            Ok(Vec::new())
        } else {
            let positions = searcher.positions(query, field, address)?;
            if positions.is_empty() {
                Ok(Vec::new())
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
                Ok(ranges)
            }
        }
    }
}
