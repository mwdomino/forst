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
    // Provides a default new method
    pub fn new() -> Self {
        Self {
            preserve_history: false,                   // Default value
            ttl: std::time::Duration::from_secs(3600), // Example default TTL
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
        Self {
            history_count: 1, // Default value
        }
    }

    // Setter method
    pub fn history_count(mut self, count: usize) -> Self {
        self.history_count = count;
        self
    }
}
