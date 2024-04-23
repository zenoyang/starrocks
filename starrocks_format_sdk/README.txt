
StarRocks Format Writer Options:
When StarRocks Lake Table Location is s3, we need the below options.
fs.s3a.endpoint: s3 endpoint
fs.s3a.endpoint.region: s3 region
fs.s3a.connection.ssl.enabled: ssl enabled or not. When s3 endpoint's prefix is https, it should be true.
fs.s3a.path.style.access: path styel access or not
fs.s3a.access.key: s3 access key
fs.s3a.secret.key: s3 security key


StarRocks Format Reader Options:
fs.s3a.endpoint: same as writer
fs.s3a.endpoint.region: same as writer
fs.s3a.connection.ssl.enabled: same as writer
fs.s3a.path.style.access: same as writer
fs.s3a.access.key: same as writer
fs.s3a.secret.key: same as writer
starrocks.format.using_column_uid:
    when read starrocks format, using column name or column uid.
    when the config value is true, using column uid; other using column name. default is false.
starrocks.format.chunk_size:
    the chunk size for read.
    default is config::vector_chunk_size, which is 4096.