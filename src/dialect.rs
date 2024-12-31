use sqlparser::dialect::Dialect;

#[derive(Debug)]
pub struct FilesDialect {}
impl Dialect for FilesDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        if ch.is_whitespace() {
            return false;
        }
        if ch.is_numeric() {
            return false;
        }
        if ch == ';' {
            return false;
        }
        if ch.is_control() {
            return false;
        }
        if ch == '.' {
            return false;
        }
        if ch == ',' {
            return false;
        }
        true
    }
    fn is_identifier_part(&self, ch: char) -> bool {
        if ch.is_whitespace() {
            return false;
        }
        if ch == ';' {
            return false;
        }
        if ch == '.' {
            return false;
        }
        if ch.is_control() {
            return false;
        }
        if ch == ',' {
            return false;
        }
        true
    }

    fn supports_numeric_prefix(&self) -> bool {
        true
    }

    fn supports_trailing_commas(&self) -> bool {
        true
    }
    fn supports_projection_trailing_commas(&self) -> bool {
        true
    }
    fn supports_limit_comma(&self) -> bool {
        true
    }
}
