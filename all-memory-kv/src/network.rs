use actix_web::{get, post, web, App, Responder, Result,HttpResponse,HttpServer};
use serde::Serialize;

pub async fn start_webserver() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
            .service(hello)
            .service(echo)
            .route("/hey", web::get().to(manual_hello))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await

}

#[get("/{key}")]
pub async fn hello(web::Path(key): web::Path<String>) -> impl Responder {
    //Get the key!
    HttpResponse::Ok().body(key)
}

#[post("/{key}")]
pub async fn echo(web::Path(key): web::Path<String>,req_body: String) -> impl Responder {
    HttpResponse::Ok().body(format!("{}:{}",key,req_body))
}

pub async fn manual_hello() -> impl Responder {
    HttpResponse::Ok().body("Hey there!")
}