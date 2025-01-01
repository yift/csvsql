use std::hash::Hash;

pub enum SmartReference<'a, T> {
    Borrowed(&'a T),
    Owned(T),
}

impl<T> std::ops::Deref for SmartReference<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        match self {
            Self::Borrowed(v) => v,
            Self::Owned(v) => v,
        }
    }
}

impl<T> std::convert::AsRef<T> for SmartReference<'_, T> {
    fn as_ref(&self) -> &T {
        self
    }
}
impl<T> From<T> for SmartReference<'_, T> {
    fn from(value: T) -> Self {
        Self::Owned(value)
    }
}
impl<'a, T> From<&'a T> for SmartReference<'a, T> {
    fn from(value: &'a T) -> Self {
        Self::Borrowed(value)
    }
}
impl<'a, T> SmartReference<'a, T> {
    pub fn duplicate(&'a self) -> Self {
        match self {
            Self::Borrowed(v) => Self::Borrowed(v),
            Self::Owned(v) => Self::Borrowed(v),
        }
    }
}
impl<T: PartialEq> PartialEq for SmartReference<'_, T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Borrowed(me), Self::Borrowed(other)) => me.eq(other),
            (Self::Borrowed(me), Self::Owned(other)) => me.eq(&other),
            (Self::Owned(me), Self::Borrowed(other)) => me.eq(other),
            (Self::Owned(me), Self::Owned(other)) => me.eq(other),
        }
    }
}
impl<T: PartialEq> Eq for SmartReference<'_, T> {}

impl<T: Hash> Hash for SmartReference<'_, T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::Borrowed(v) => v.hash(state),
            Self::Owned(v) => v.hash(state),
        }
    }
}
