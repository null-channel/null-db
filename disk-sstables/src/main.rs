use actix_web::{get, post, web, App, Responder, Result,HttpResponse,HttpServer};
#[macro_use]
extern crate lazy_static;
use std::sync::RwLock; // read heavy -- probably better period.

lazy_static! {
    static ref LOCK: RwLock<bool> = {
        let mut m = false;
        RwLock::new(m)
    };
}
#[actix_web::main]
async fn main() -> std::io::Result<()> {
        HttpServer::new(|| {
            App::new()
                .service(get_value_for_key)
                .service(put_value_for_key)
        })
        .bind("127.0.0.1:8080")?
        .run()
        .await
    
}

#[get("/{key}")]
pub async fn get_value_for_key(web::Path(key): web::Path<String>) -> impl Responder {
    //Get the key!
    HttpResponse::Ok().body(map.get(&key).unwrap())
}

#[post("/{key}")]
pub async fn put_value_for_key(web::Path(key): web::Path<String>,req_body: String) -> impl Responder {

    HttpResponse::Ok().body("It is saved... in memory!")
}

pub async fn manual_hello() -> impl Responder {
    HttpResponse::Ok().body("Hey there!")
}