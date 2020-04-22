# rust-lambda-test

This is a toy example of a Rust application for deployment on AWS Lambda.

This repo accompanies [this blog post](http://jamesmcm.github.io/blog/2020/04/19/data-engineering-with-rust-and-aws-lambda/#en).

It loads an Excel file from S3 to a Redshift cluster, on-demand, triggered when the file is uploaded.

You can run `cargo test` to run just the CSV generation from a test Excel file locally.

## Details

The Excel file we want to parse has the following structure in the `data` worksheet:

| location | metric          | value | date       |
|----------|-----------------|-------|------------|
| UK       | conversion\_rate | 0     | 2020-02-01 |
| ES       | conversion\_rate | 0.634 | 2020-02-01 |
| DE       | conversion\_rate | #N/A  | 2020-02-01 |
| FR       | conversion\_rate | #N/A  | 2020-02-01 |
| UK       | conversion\_rate | 0.723 | 2020-01-31 |

We only want to import rows which have the same date as the first row in the Excel file.

Note the possibility of `#N/A` and invalid values which we will need to handle.

This file will be uploaded to `s3://input-bucket-name/label/filename.xlsx` where the label allows multiple files to be uploaded (with different locations in each file).

The output CSV is written to `s3://output-bucket-name/label/YYYY-MM-DD.csv` where the date of the file is the date of the first row.

The CSV is loaded to the `public.test_table` test.

Note to actually run this you will need to edit the code to provide an IAM role for the COPY command (and replace the placeholder bucket names, etc.)

## Deployment

To deploy the function, follow the instructions on [the AWS blog about the Rust runtime](https://aws.amazon.com/blogs/opensource/rust-runtime-for-aws-lambda/).

Note connecting via SSL to a Redshift cluster requires the CA certificate, which is included in this repo. In the case of changes, it is available at: https://s3.amazonaws.com/redshift-downloads/redshift-ssl-ca-cert.pem


### OS X cross-compilation

If you are building on OS X you need to cross-compile the binary, as per the instructions in the blog post.

```bash
rustup target add x86_64-unknown-linux-musl
brew install filosottile/musl-cross/musl-cross
mkdir .cargo
echo '[target.x86_64-unknown-linux-musl]
linker = "x86_64-linux-musl-gcc"' > .cargo/config
ln -s /usr/local/bin/x86_64-linux-musl-gcc /usr/local/bin/musl-gcc
```

Note the installation of musl-cross took almost 2 hours on my machine.

### Build zip artefact

Remember to add the Redshift CA certificate to the zip archive:

```bash
cargo build --release --target x86_64-unknown-linux-musl
zip -j rust.zip ./target/x86_64-unknown-linux-musl/release/bootstrap ./redshift-ssl-ca-cert.pem
```

### Upload to AWS

Create a new Lambda function with a Custom runtime, and then upload the zip file.

Note if your Redshift cluster (or RDS instance) is behind a VPC you will need to add the Lambda function to the same VPC. See [the documentation](https://docs.aws.amazon.com/lambda/latest/dg/configuration-vpc.html) for more details. Then add the S3 trigger to the Lambda function (and you can test the funtion using the S3 event template).

