use actix_web::{get, post,delete, web::{self, Data}, App, Responder,HttpResponse,HttpServer};
#[macro_use]
extern crate lazy_static;
use std::collections::HashMap;
use std::sync::RwLock; // read heavy -- probably better period.


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let mut data = Data::new({
        let mut m = HashMap::new();
        // Pre-fill the db with some values
        m.insert("foo".to_owned(), "foo".to_owned());
        m.insert("bar".to_owned(), "baz".to_owned());
        m.insert("bax".to_owned(), "baz".to_owned());
        RwLock::new(m)
    });
    HttpServer::new(move || {
        App::new()
            .app_data(data.clone())
            .service(get_value_for_key)
            .service(put_value_for_key)
            .service(delete_value_for_key)
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}

#[get("/{key}")]
pub async fn get_value_for_key(
    data: Data<RwLock<HashMap<String, String>>>,
    web::Path(key): web::Path<String>
) -> impl Responder {
    //Get the key!
    let map = data.read().unwrap();
    HttpResponse::Ok().body(map.get(&key).unwrap())
}

#[post("/{key}")]
pub async fn put_value_for_key(
    data: Data<RwLock<HashMap<String, String>>>,
    web::Path(key): web::Path<String>,req_body: String
) -> impl Responder {
    let mut map = data.write().unwrap();
    map.insert(key,req_body);
    HttpResponse::Ok().body("It is saved... in memory!")
}

#[delete("/{key}")]
pub async fn delete_value_for_key(
    data: Data<RwLock<HashMap<String, String>>>,
    web::Path(key): web::Path<String>
) -> impl Responder {
    let mut map = data.write().unwrap();
    map.remove(&key);
    HttpResponse::Ok().body("It has been deleted!")
}