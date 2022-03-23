

#[actix_web::main]
async fn main() -> std::io::Result<()> {

    let file_mutex = Data::new(RwLock::new("null.db"));

    HttpServer::new(move || {
        App::new()
            .app_data(file_mutex.clone())
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
    file_mutex: Data<RwLock<&str>>, 
    web::Path(key): web::Path<String>
) -> impl Responder {
    return HttpResponse::NotImplemented();
}

#[post("/{key}")]
pub async fn put_value_for_key(
    file_mutex: Data<RwLock<&str>>,
    web::Path(key): web::Path<String>,
    req_body: String
) -> impl Responder {
    let writer = file_mutex.write().unwrap();
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(*writer)
        .unwrap();

    if let Err(e) = writeln!(file, "{}:{}", key, req_body) {
        eprintln!("Could not write to file: {}", e);
        return HttpResponse::InternalServerError();
    }
    return HttpResponseBuilder::from(HttpResponse::Ok().body("Saved Value to disk"));
}

#[delete("/{key}")]
pub async fn delete_value_for_key(
    file_mutex: Data<RwLock<&str>>,
    web::Path(key): web::Path<String>
) -> impl Responder {
    return HttpResponse::NotImplemented();
}

