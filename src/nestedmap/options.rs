#[derive(Debug, Clone)]
pub struct SetOptions {
    pub preserve_history: bool,
    pub ttl: std::time::Duration,
}

impl Default for SetOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl SetOptions {
    pub fn new() -> Self {
        Self {
            preserve_history: false,
            ttl: std::time::Duration::from_secs(3600),
        }
    }

    // Methods to set options
    pub fn preserve_history(mut self, value: bool) -> Self {
        self.preserve_history = value;
        self
    }

    pub fn ttl(mut self, value: std::time::Duration) -> Self {
        self.ttl = value;
        self
    }
}

#[derive(Debug, Clone)]
pub struct GetOptions {
    pub history_count: usize,
}

impl Default for GetOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl GetOptions {
    // Default constructor
    pub fn new() -> Self {
        Self { history_count: 1 }
    }

    // Setter method
    pub fn history_count(mut self, count: usize) -> Self {
        self.history_count = count;
        self
    }
}
