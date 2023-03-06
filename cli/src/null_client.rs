use reqwest::{Error, Response};

pub struct NullClient {
    url: String,
    client: reqwest::Client,
}

impl NullClient {
    pub fn new(url: String) -> Self {
        NullClient {
            url,
            client: reqwest::Client::new(),
        }
    }

    pub async fn post(&self, key: String, data: String) -> Result<Response, Error> {
        let body = data.clone();
        self.client
            .post(format!("{}/{}\n", self.url, key))
            .body(body)
            .send()
            .await
    }
}
