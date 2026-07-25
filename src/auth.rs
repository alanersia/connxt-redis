use crate::{connection::Connection, error::Result};
pub fn authenticate(c: &mut Connection, username: Option<&str>, password: &str) -> Result<()> {
    let cmd = if let Some(u) = username {
        crate::protocol::Command::new("AUTH").arg(u).arg(password)
    } else {
        crate::protocol::Command::new("AUTH").arg(password)
    };
    c.execute(cmd).map(|_| ())
}
