pub type GenericError = Box<dyn Error + Send + Sync>;
pub type GenericResult<T> = Result<T, GenericError>;
