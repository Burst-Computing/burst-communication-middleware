#[derive(Debug, Clone, PartialEq)]
pub struct BurstMiddleware {
    pub burst_size: i32,
    pub worker_id: i32,
}
