

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
    let reader = *file_mutex.read().unwrap();
    let file = File::open(reader).unwrap();
    let mut reader = EasyReader::new(file).unwrap();

    reader.build_index();
    reader.eof();

    while let Some(line) = reader.prev_line().unwrap() {
        let split = line.split(":").collet::<Vec<&str>>();
        if (split.len() == 2) { //just quick sanity check
            let value = split[1].to_string().clone();
            return HttpResponse::Ok().body(value);
        }
    }
    return HttpResponse::NotFound();
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

const TOMBSTONE: &'static str = "~tombstone~";

#[delete("/{key}")]
pub async fn delete_value_for_key(
    file_mutex: Data<RwLock<&str>>,
    web::Path(key): web::Path<String>
) -> impl Responder {    
    let writer = file_mutex.write().unwrap();
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(*writer)
        .unwrap();

    if let Err(e) = writeln!(file, "{}:{}", key, TOMBSTONE) {
        eprintln!("Could not delete key: {}", e);
        return HttpResponse::InternalServerError();
    }
    return HttpResponseBuilder::from(HttpResponse::Ok().body("Record Deleted"));
}

