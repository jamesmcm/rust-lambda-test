use calamine::{RangeDeserializerBuilder, Reader, Xlsx};
use csv::Writer;
use lambda_runtime::error::HandlerError;
use openssl::ssl::{SslConnector, SslMethod};
use percent_encoding::percent_decode_str;
use postgres::Client;
use postgres_openssl::MakeTlsConnector;
use rusoto_core::Region;
use rusoto_s3::{GetObjectRequest, PutObjectRequest, S3Client, S3};
use rusoto_secretsmanager::{GetSecretValueRequest, SecretsManager, SecretsManagerClient};
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashSet;
use std::error::Error;
use std::io::Cursor;
use std::io::Read;

const INPUT_BUCKET: &str = "input-bucket-name";
const OUTPUT_BUCKET: &str = "output-bucket-name";
const COLUMNS: [&str; 4] = ["location", "metric", "value", "date"];

#[derive(Serialize, Deserialize, Debug)]
struct RawExcelRow {
    location: String,
    metric: String,
    #[serde(deserialize_with = "de_opt_f64")]
    value: Option<f64>,
    #[serde(deserialize_with = "de_date")]
    date: chrono::NaiveDate,
}

fn de_opt_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let data_type = calamine::DataType::deserialize(deserializer);
    match data_type {
        Ok(calamine::DataType::Error(_)) => Ok(None),
        Ok(calamine::DataType::Float(f)) => Ok(Some(f)),
        Ok(calamine::DataType::Int(i)) => Ok(Some(i as f64)),
        _ => Ok(None),
    }
}

fn de_date<'de, D>(deserializer: D) -> Result<chrono::NaiveDate, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let data_type = calamine::DataType::deserialize(deserializer);
    match data_type {
        Ok(x) => x.as_date().ok_or(serde::de::Error::custom("Invalid Date")),
        Err(x) => Err(x),
    }
}

#[allow(non_snake_case)]
#[derive(Serialize, Deserialize, Debug)]
struct DBCredentials {
    username: String,
    password: String,
    engine: String,
    host: String,
    port: u32,
    dbClusterIdentifier: String,
}

fn get_excel_from_s3(
    bucket: &str,
    key: &str,
) -> Result<Xlsx<Cursor<Vec<u8>>>, Box<dyn std::error::Error>> {
    let mut buffer: Vec<u8> = Vec::new();
    let s3_client = S3Client::new(Region::EuWest1);

    println!("Reading bucket: {}, key: {}", bucket, key);
    let s3file = s3_client
        .get_object(GetObjectRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            ..Default::default()
        })
        .sync()?;

    let _file = s3file
        .body
        .unwrap()
        .into_blocking_read()
        .read_to_end(&mut buffer)?;
    Ok(Xlsx::new(Cursor::new(buffer))?)
}

fn excel_to_csv_string(
    mut excel: Xlsx<Cursor<Vec<u8>>>,
) -> Result<(String, HashSet<String>, chrono::NaiveDate), Box<dyn std::error::Error>> {
    let range = excel
        .worksheet_range("data")
        .ok_or(calamine::Error::Msg("Cannot find data worksheet"))??;

    let mut iter_result =
        RangeDeserializerBuilder::with_headers(&COLUMNS).from_range::<_, RawExcelRow>(&range)?;

    // Use date of first row as date for file
    let mut wtr = Writer::from_writer(vec![]);
    let mut locations: HashSet<String> = HashSet::new();

    let first_row = iter_result.next().unwrap()?;
    let canonical_date = first_row.date.clone();
    locations.insert(first_row.location.clone());
    wtr.serialize(first_row)?;
    println!("Canonical date: {:?}", canonical_date);

    for (index, row) in iter_result.enumerate() {
        match row {
            Ok(row) => {
                if row.date == canonical_date {
                    locations.insert(row.location.clone());
                    wtr.serialize(row)?;
                }
            }
            Err(row) => println!("{}: {:?}", index + 2, row),
        }
    }

    let data = String::from_utf8(wtr.into_inner()?)?;

    Ok((data, locations, canonical_date))
}

fn upload_csv_to_s3(
    data: String,
    label: &str,
    canonical_date: &chrono::NaiveDate,
) -> Result<String, Box<dyn std::error::Error>> {
    let outputkey = format!(
        "output/{}_{}.csv",
        label,
        canonical_date.format("%Y-%m-%d").to_string()
    );

    // Write CSV to S3
    let s3_client = S3Client::new(Region::EuWest1);
    s3_client
        .put_object(PutObjectRequest {
            bucket: String::from(OUTPUT_BUCKET),
            key: outputkey.clone(),
            body: Some(data.into_bytes().into()),
            ..Default::default()
        })
        .sync()?;

    Ok(outputkey)
}

fn get_db_credentials() -> Result<DBCredentials, Box<dyn std::error::Error>> {
    let sm_client = SecretsManagerClient::new(Region::EuWest1);
    let secret = sm_client
        .get_secret_value(GetSecretValueRequest {
            secret_id: "db_credentials_secret".to_string(),
            version_id: None,
            version_stage: None,
        })
        .sync()?;

    let credentials: DBCredentials = serde_json::from_str(&secret.secret_string.unwrap())?;

    Ok(credentials)
}

fn load_to_db(
    outputkey: &str,
    canonical_date: &chrono::NaiveDate,
    locations: &HashSet<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = SslConnector::builder(SslMethod::tls())?;
    builder.set_ca_file("redshift-ssl-ca-cert.pem")?;
    let connector = MakeTlsConnector::new(builder.build());

    let credentials = get_db_credentials()?;
    let mut client = Client::connect(
        format!(
            "host={} port={} dbname={} user={} password={} sslmode=require",
            credentials.host,
            credentials.port,
            "dbname",
            credentials.username,
            credentials.password
        )
        .as_str(),
        connector,
    )?;

    let locations_vec: Vec<String> = locations
        .iter()
        .cloned()
        .map(|x| format!("'{}'", x))
        .collect();

    let target_table = "test_table";

    let location_string = &locations_vec.join(",");
    let truncate_query = format!(
        "DELETE FROM public.{} WHERE date = '{}' AND location IN ({});",
        target_table,
        canonical_date.format("%Y-%m-%d").to_string(),
        location_string
    );
    let colstr = &COLUMNS.join(",");
    println!("{}", truncate_query);
    let copy_query = format!(
        "COPY public.{} ({}) from
                 's3://{}/{}'
                  iam_role 'arn:aws:iam::YOUR_ROLE_HERE'
                  FORMAT CSV
                  EMPTYASNULL
                  BLANKSASNULL
                  IGNOREHEADER 1
                  IGNOREBLANKLINES
                  ;",
        target_table, colstr, OUTPUT_BUCKET, outputkey
    );
    println!("{}", copy_query);
    println!("{:?}", client.execute(truncate_query.as_str(), &[]));
    println!("{:?}", client.execute(copy_query.as_str(), &[]));

    Ok(())
}

fn handle_excel(key: &str) -> Result<(), Box<dyn std::error::Error>> {
    let label = key.split("/").nth(1).unwrap();
    let excel: Xlsx<_> = get_excel_from_s3(INPUT_BUCKET, &key)?;

    let (data, locations, canonical_date) = excel_to_csv_string(excel)?;

    let outputkey = upload_csv_to_s3(data, label, &canonical_date)?;

    load_to_db(&outputkey, &canonical_date, &locations)?;
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    lambda_runtime::lambda!(my_handler);

    Ok(())
}

fn my_handler(
    e: aws_lambda_events::event::s3::S3Event,
    _c: lambda_runtime::Context,
) -> Result<(), HandlerError> {
    println!("{:?}", e);
    let decodedkey = percent_decode_str(&(e.records[0].s3.object.key.as_ref()).unwrap())
        .decode_utf8()
        .unwrap();

    match handle_excel(&decodedkey) {
        Ok(_) => (),
        Err(error) => {
            panic!("Error: {:?}", error);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;

    #[test]
    fn test_local() -> Result<(), Box<dyn Error>> {
        let mut buffer: Vec<u8> = Vec::new();
        let mut f = File::open(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(PathBuf::from("tests/test_excel.xlsx")),
        )?;
        f.read_to_end(&mut buffer)?;

        let excel = Xlsx::new(Cursor::new(buffer))?;
        let (data, locations, canonical_date) = excel_to_csv_string(excel)?;

        let mut test_set = HashSet::with_capacity(2);
        test_set.insert(String::from("UK"));
        test_set.insert(String::from("FR"));

        assert_eq!(locations, test_set);

        assert_eq!(
            canonical_date,
            chrono::naive::NaiveDate::from_ymd(2020, 2, 1)
        );

        {
            let mut file = File::create(
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join(PathBuf::from("tests/test_output.csv")),
            )?;
            file.write_all(data.as_bytes())?;
        }

        let read_csv = std::fs::read_to_string(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(PathBuf::from("tests/test_output.csv")),
        )?;

        assert_eq!(read_csv, data);

        Ok(())
    }
}
