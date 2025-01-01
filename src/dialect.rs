use sqlparser::dialect::Dialect;

#[derive(Debug)]
pub struct FilesDialect {}
impl Dialect for FilesDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        if ch.is_numeric() {
            return false;
        }
        self.is_identifier_part(ch)
    }
    fn is_identifier_part(&self, ch: char) -> bool {
        if ch.is_alphanumeric() {
            return true;
        }
        if ch == '_' {
            return true;
        }
        if ch == '$' {
            return true;
        }
        false
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
