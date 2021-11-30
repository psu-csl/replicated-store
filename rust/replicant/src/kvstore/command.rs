pub enum Command<'a> {
    Get(&'a str),
    Put(String, String),
    Del(&'a str),
}
