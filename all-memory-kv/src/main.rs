use actix_web::{web, App, HttpRequest, HttpServer, Responder};
mod network;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    network::start_webserver().await
}