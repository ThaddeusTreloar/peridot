pub(crate) fn topic_partition_offset(topic: &str, partition: i32, offset: i64) -> String {
    format!(
        "topic: {}, partition: {}, offset: {}",
        topic, partition, offset
    )
}

pub(crate) fn topic_partition_offset_err(
    topic: &str,
    partition: i32,
    offset: i64,
    err: &impl std::error::Error,
) -> String {
    format!(
        "topic: {}, partition: {}, offset: {}, caused by: {}",
        topic, partition, offset, err
    )
}
