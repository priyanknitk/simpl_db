use crate::constants::*;

pub struct Row {
    pub id: i32,
    pub username: String,
    pub email: String,
}

impl Row {
    pub fn serialize_row(&self, destination: &mut [u8]) {
        let id_bytes = self.id.to_le_bytes();
        let username_bytes = self.username.as_bytes();
        let email_bytes = self.email.as_bytes();

        destination[ID_OFFSET..ID_OFFSET + id_bytes.len()].copy_from_slice(&id_bytes);
        destination[USERNAME_OFFSET..USERNAME_OFFSET + username_bytes.len()]
            .copy_from_slice(username_bytes);
        destination[EMAIL_OFFSET..EMAIL_OFFSET + email_bytes.len()].copy_from_slice(email_bytes);
    }

    pub fn deserialize_row(source: &[u8]) -> Self {
        let id = i32::from_le_bytes(source[ID_OFFSET..ID_OFFSET + ID_SIZE].try_into().unwrap());
        let username =
            std::str::from_utf8(&source[USERNAME_OFFSET..USERNAME_OFFSET + USERNAME_SIZE])
                .unwrap()
                .to_string();
        let email = std::str::from_utf8(&source[EMAIL_OFFSET..EMAIL_OFFSET + EMAIL_SIZE])
            .unwrap()
            .to_string();

        Row {
            id,
            username,
            email,
        }
    }
}
