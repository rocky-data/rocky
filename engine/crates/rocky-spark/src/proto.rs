//! The slice of the Spark Connect protocol this adapter speaks.
//!
//! Spark Connect is gRPC with protobuf messages (package `spark.connect`).
//! Rocky sends one kind of request — `ExecutePlan` with a plan whose root is
//! a SQL relation — and reads two kinds of response: Arrow batches and the
//! end-of-results marker. The messages below declare only the fields Rocky
//! reads or writes, with the field numbers of the upstream `.proto` files
//! (Spark 3.4 through 4.x keep them stable). Protobuf decoding skips the
//! fields a message does not declare, so the server's other response fields
//! (metrics, progress, schema) are ignored rather than rejected.
//!
//! Declaring the subset by hand keeps the crate free of `protoc` and of a
//! build script; the RPC goes through `tonic`'s generic gRPC client with the
//! `prost` codec, exactly as generated code would.

/// `spark.connect.ExecutePlanRequest`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ExecutePlanRequest {
    /// A client-chosen UUID. Every statement of one adapter shares it, so
    /// session state (temporary views, `SET` options) carries over.
    #[prost(string, tag = "1")]
    pub session_id: String,
    #[prost(message, optional, tag = "2")]
    pub user_context: Option<UserContext>,
    #[prost(message, optional, tag = "3")]
    pub plan: Option<Plan>,
    /// Free-form client identifier, shown in the Spark UI.
    #[prost(string, optional, tag = "4")]
    pub client_type: Option<String>,
    /// A client-chosen UUID naming this one execution.
    #[prost(string, optional, tag = "6")]
    pub operation_id: Option<String>,
}

/// `spark.connect.UserContext`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct UserContext {
    #[prost(string, tag = "1")]
    pub user_id: String,
    #[prost(string, tag = "2")]
    pub user_name: String,
}

/// `spark.connect.Plan`. Only the `root` (relation) arm is declared.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Plan {
    #[prost(oneof = "plan::OpType", tags = "1")]
    pub op_type: Option<plan::OpType>,
}

pub mod plan {
    /// `spark.connect.Plan.op_type`.
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum OpType {
        #[prost(message, tag = "1")]
        Root(super::Relation),
    }
}

/// `spark.connect.Relation`. Only the `sql` arm is declared.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Relation {
    #[prost(oneof = "relation::RelType", tags = "10")]
    pub rel_type: Option<relation::RelType>,
}

pub mod relation {
    /// `spark.connect.Relation.rel_type`.
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum RelType {
        #[prost(message, tag = "10")]
        Sql(super::Sql),
    }
}

/// `spark.connect.SQL`: the relation `spark.sql(query)` builds. Executing a
/// plan rooted at it runs the statement — a command (DDL, DML) runs eagerly
/// when the server analyses it, and a query streams its rows back.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Sql {
    #[prost(string, tag = "1")]
    pub query: String,
}

/// `spark.connect.ExecutePlanResponse`. Only the Arrow-batch and
/// result-complete arms of `response_type` are declared; any other arm
/// decodes as `None`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ExecutePlanResponse {
    #[prost(string, tag = "1")]
    pub session_id: String,
    #[prost(oneof = "execute_plan_response::ResponseType", tags = "2, 14")]
    pub response_type: Option<execute_plan_response::ResponseType>,
}

pub mod execute_plan_response {
    /// `spark.connect.ExecutePlanResponse.response_type`.
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum ResponseType {
        #[prost(message, tag = "2")]
        ArrowBatch(super::ArrowBatch),
        #[prost(message, tag = "14")]
        ResultComplete(super::ResultComplete),
    }
}

/// `spark.connect.ExecutePlanResponse.ArrowBatch`: one Arrow IPC stream
/// (schema message plus record batches).
#[derive(Clone, PartialEq, prost::Message)]
pub struct ArrowBatch {
    #[prost(int64, tag = "1")]
    pub row_count: i64,
    #[prost(bytes = "vec", tag = "2")]
    pub data: Vec<u8>,
}

/// `spark.connect.ExecutePlanResponse.ResultComplete`: the server sent every
/// result. A stream that ends without it was cut short.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct ResultComplete {}

/// The fully qualified gRPC method path of `ExecutePlan`.
pub const EXECUTE_PLAN_PATH: &str = "/spark.connect.SparkConnectService/ExecutePlan";

/// Build the request that runs `sql` as one plan.
#[must_use]
pub fn sql_request(
    session_id: &str,
    user_id: &str,
    operation_id: &str,
    client_type: &str,
    sql: &str,
) -> ExecutePlanRequest {
    ExecutePlanRequest {
        session_id: session_id.to_string(),
        user_context: Some(UserContext {
            user_id: user_id.to_string(),
            user_name: user_id.to_string(),
        }),
        plan: Some(Plan {
            op_type: Some(plan::OpType::Root(Relation {
                rel_type: Some(relation::RelType::Sql(Sql {
                    query: sql.to_string(),
                })),
            })),
        }),
        client_type: Some(client_type.to_string()),
        operation_id: Some(operation_id.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;

    /// The bytes must match what the upstream generated code emits for the
    /// same request: field 1 session id, 2 user context, 3 plan → root (1) →
    /// sql (10) → query (1), 4 client type, 6 operation id.
    #[test]
    fn sql_request_uses_upstream_field_numbers() {
        let req = sql_request("s", "u", "o", "c", "SELECT 1");
        let bytes = req.encode_to_vec();
        let expected: Vec<u8> = [
            &[0x0a, 0x01, b's'][..],
            // user_context: user_id "u", user_name "u"
            &[0x12, 0x06, 0x0a, 0x01, b'u', 0x12, 0x01, b'u'][..],
            // plan { root { sql { query "SELECT 1" } } }
            &[0x1a, 0x0e, 0x0a, 0x0c, 0x52, 0x0a, 0x0a, 0x08][..],
            b"SELECT 1",
            &[0x22, 0x01, b'c'][..],
            &[0x32, 0x01, b'o'][..],
        ]
        .concat();
        assert_eq!(bytes, expected);
    }

    #[test]
    fn response_decodes_arrow_batch_and_skips_undeclared_fields() {
        // ArrowBatch (field 2) with data [1,2,3], plus an undeclared
        // `operation_id` (field 12) and `schema` (field 7) the server sends.
        let mut bytes = vec![0x0a, 0x01, b's'];
        bytes.extend_from_slice(&[0x12, 0x07, 0x08, 0x02, 0x12, 0x03, 1, 2, 3]);
        bytes.extend_from_slice(&[0x62, 0x01, b'x']);
        bytes.extend_from_slice(&[0x3a, 0x00]);
        let resp = ExecutePlanResponse::decode(bytes.as_slice()).unwrap();
        assert_eq!(resp.session_id, "s");
        match resp.response_type {
            Some(execute_plan_response::ResponseType::ArrowBatch(b)) => {
                assert_eq!(b.row_count, 2);
                assert_eq!(b.data, vec![1, 2, 3]);
            }
            other => panic!("expected an Arrow batch, got {other:?}"),
        }
    }

    #[test]
    fn response_decodes_result_complete_and_unknown_arms_as_none() {
        let done = ExecutePlanResponse::decode(&[0x72, 0x00][..]).unwrap();
        assert!(matches!(
            done.response_type,
            Some(execute_plan_response::ResponseType::ResultComplete(_))
        ));
        // `sql_command_result` (field 5) is an arm Rocky does not declare.
        let other = ExecutePlanResponse::decode(&[0x2a, 0x00][..]).unwrap();
        assert_eq!(other.response_type, None);
    }
}
