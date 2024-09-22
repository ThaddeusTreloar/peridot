pub mod engine;

pub enum ErrorType<T> {
    Fatal(T),
    Retryable(T),
}

pub enum Retryable<T> {
    E(T),
}

impl <T> Retryable<T> {
    pub fn into_inner(self) -> T {
        match self {
            Self::E(e) => e
        }
    }
}

pub enum Fatal<T> {
    E(T),
}

impl <T> Fatal<T> {
    pub fn into_inner(self) -> T {
        match self {
            Self::E(e) => e
        }
    }
}

pub trait Retry 
where Self: Sized
{
    fn retry(self, limit: Option<usize>) -> Self {
        let l = match limit {
            Some(l) => l,
            None => usize::MAX
        };

        for _ in 0..l {
            todo!("")
        }

        self
    }
}