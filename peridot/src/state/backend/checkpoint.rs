#[derive(Debug, Default, Clone)]
pub struct Checkpoint {
    pub store_name: String,
    pub offset: i64,
}

impl Checkpoint {
    pub fn new(store_name: String, offset: i64) -> Self {
        Self { store_name, offset }
    }

    pub fn set_offset(&mut self, new_offset: i64) {
        self.offset = new_offset
    }

    pub fn set_offset_if_greater(&mut self, maybe_new_offset: i64) {
        if maybe_new_offset > self.offset {
            self.offset = maybe_new_offset
        }
    }
}
