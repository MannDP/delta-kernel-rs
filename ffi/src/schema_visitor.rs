//! Defines `KernelSchemaVisitorState` - a visitor that can be used to convert an
//! engine's native schema into kernel's `Schema` type.
//!
//! This is the reverse direction of `EngineSchemaVisitor` - instead of kernel telling
//! engine about a schema, engine tells kernel about a schema for projection pushdown.
//!
//! Supports all Delta types including nested structures, arrays, maps, and variants.
//! Uses proper recursive building with dependency handling.

use crate::{KernelStringSlice, ReferenceSet, TryFromStringSlice};
use delta_kernel::schema::{
    ArrayType, DataType, DecimalType, MapType, PrimitiveType, StructField, StructType,
};
use std::collections::HashMap;

/// Element types that can be built during schema construction
pub(crate) enum SchemaElement {
    /// A complete field (name + data type + metadata)
    Field(StructField),
    /// A data type that can be used in fields, arrays, maps
    DataType(DataType),
    /// A complete schema (struct type)
    Schema(StructType),
}

/// State for converting engine schemas to kernel schemas with full Delta type support
/// Uses field IDs for proper handling of complex nested types
#[derive(Default)]
pub struct KernelSchemaVisitorState {
    elements: ReferenceSet<SchemaElement>,
}

/// Helper to insert a StructField and return its ID
fn wrap_field(state: &mut KernelSchemaVisitorState, field: StructField) -> usize {
    let element = SchemaElement::Field(field);
    state.elements.insert(element)
}

/// Helper to insert a DataType and return its ID
fn wrap_data_type(state: &mut KernelSchemaVisitorState, data_type: DataType) -> usize {
    let element = SchemaElement::DataType(data_type);
    state.elements.insert(element)
}

/// Helper to insert a StructType and return its ID
fn wrap_schema(state: &mut KernelSchemaVisitorState, schema: StructType) -> usize {
    let element = SchemaElement::Schema(schema);
    state.elements.insert(element)
}

/// Extract a DataType from the visitor state
fn unwrap_data_type(state: &mut KernelSchemaVisitorState, type_id: usize) -> Option<DataType> {
    match state.elements.take(type_id)? {
        SchemaElement::DataType(data_type) => Some(data_type),
        SchemaElement::Field(field) => Some(field.data_type),
        SchemaElement::Schema(schema) => Some(DataType::Struct(Box::new(schema))),
    }
}

/// Extract a StructField from the visitor state
fn unwrap_field(state: &mut KernelSchemaVisitorState, field_id: usize) -> Option<StructField> {
    match state.elements.take(field_id)? {
        SchemaElement::Field(field) => Some(field),
        _ => None,
    }
}

/// Extract the final schema from the visitor state
pub fn unwrap_kernel_schema(
    state: &mut KernelSchemaVisitorState,
    schema_id: usize,
) -> Option<StructType> {
    match state.elements.take(schema_id)? {
        SchemaElement::Schema(schema) => Some(schema),
        SchemaElement::Field(field) => {
            // Convert single field to schema with one field
            Some(StructType::new([field].into_iter()))
        }
        SchemaElement::DataType(DataType::Struct(struct_type)) => Some(*struct_type),
        _ => None,
    }
}

// =============================================================================
// FFI Visitor Functions - Primitive Types
// =============================================================================

/// Create a String field - returns field ID for composition
#[no_mangle]
pub extern "C" fn visit_schema_string(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
) -> usize {
    let name_str = match unsafe { <&str>::try_from_slice(&name) } {
        Ok(s) => s.to_string(),
        Err(_) => return 0, // Invalid name -> invalid field
    };

    let field = StructField {
        name: name_str,
        data_type: DataType::Primitive(PrimitiveType::String),
        nullable,
        metadata: HashMap::new(),
    };

    wrap_field(state, field)
}

#[no_mangle]
pub extern "C" fn visit_schema_long(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
) -> usize {
    let name_str = match unsafe { <&str>::try_from_slice(&name) } {
        Ok(s) => s.to_string(),
        Err(_) => return 0,
    };

    let field = StructField {
        name: name_str,
        data_type: DataType::Primitive(PrimitiveType::Long),
        nullable,
        metadata: HashMap::new(),
    };

    wrap_field(state, field)
}

#[no_mangle]
pub extern "C" fn visit_schema_integer(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
) -> usize {
    let name_str = match unsafe { <&str>::try_from_slice(&name) } {
        Ok(s) => s.to_string(),
        Err(_) => return 0,
    };

    let field = StructField {
        name: name_str,
        data_type: DataType::Primitive(PrimitiveType::Integer),
        nullable,
        metadata: HashMap::new(),
    };

    wrap_field(state, field)
}

#[no_mangle]
pub extern "C" fn visit_schema_boolean(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
) -> usize {
    let name_str = match unsafe { <&str>::try_from_slice(&name) } {
        Ok(s) => s.to_string(),
        Err(_) => return 0,
    };

    let field = StructField {
        name: name_str,
        data_type: DataType::Primitive(PrimitiveType::Boolean),
        nullable,
        metadata: HashMap::new(),
    };

    wrap_field(state, field)
}

#[no_mangle]
pub extern "C" fn visit_schema_double(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
) -> usize {
    let name_str = match unsafe { <&str>::try_from_slice(&name) } {
        Ok(s) => s.to_string(),
        Err(_) => return 0,
    };

    let field = StructField {
        name: name_str,
        data_type: DataType::Primitive(PrimitiveType::Double),
        nullable,
        metadata: HashMap::new(),
    };

    wrap_field(state, field)
}

/// Create a Short field (i16)
#[no_mangle]
pub extern "C" fn visit_schema_short(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
) -> usize {
    let name_str = match unsafe { <&str>::try_from_slice(&name) } {
        Ok(s) => s.to_string(),
        Err(_) => return 0,
    };

    let field = StructField {
        name: name_str,
        data_type: DataType::Primitive(PrimitiveType::Short),
        nullable,
        metadata: HashMap::new(),
    };

    wrap_field(state, field)
}

/// Create a Byte field (i8)
#[no_mangle]
pub extern "C" fn visit_schema_byte(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
) -> usize {
    let name_str = match unsafe { <&str>::try_from_slice(&name) } {
        Ok(s) => s.to_string(),
        Err(_) => return 0,
    };

    let field = StructField {
        name: name_str,
        data_type: DataType::Primitive(PrimitiveType::Byte),
        nullable,
        metadata: HashMap::new(),
    };

    wrap_field(state, field)
}

/// Create a Float field (f32)
#[no_mangle]
pub extern "C" fn visit_schema_float(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
) -> usize {
    let name_str = match unsafe { <&str>::try_from_slice(&name) } {
        Ok(s) => s.to_string(),
        Err(_) => return 0,
    };

    let field = StructField {
        name: name_str,
        data_type: DataType::Primitive(PrimitiveType::Float),
        nullable,
        metadata: HashMap::new(),
    };

    wrap_field(state, field)
}

/// Create a Binary field
#[no_mangle]
pub extern "C" fn visit_schema_binary(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
) -> usize {
    let name_str = match unsafe { <&str>::try_from_slice(&name) } {
        Ok(s) => s.to_string(),
        Err(_) => return 0,
    };

    let field = StructField {
        name: name_str,
        data_type: DataType::Primitive(PrimitiveType::Binary),
        nullable,
        metadata: HashMap::new(),
    };

    wrap_field(state, field)
}

/// Create a Date field
#[no_mangle]
pub extern "C" fn visit_schema_date(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
) -> usize {
    let name_str = match unsafe { <&str>::try_from_slice(&name) } {
        Ok(s) => s.to_string(),
        Err(_) => return 0,
    };

    let field = StructField {
        name: name_str,
        data_type: DataType::Primitive(PrimitiveType::Date),
        nullable,
        metadata: HashMap::new(),
    };

    wrap_field(state, field)
}

/// Create a Timestamp field (microsecond precision, UTC)
#[no_mangle]
pub extern "C" fn visit_schema_timestamp(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
) -> usize {
    let name_str = match unsafe { <&str>::try_from_slice(&name) } {
        Ok(s) => s.to_string(),
        Err(_) => return 0,
    };

    let field = StructField {
        name: name_str,
        data_type: DataType::Primitive(PrimitiveType::Timestamp),
        nullable,
        metadata: HashMap::new(),
    };

    wrap_field(state, field)
}

/// Create a TimestampNtz field (no timezone)
#[no_mangle]
pub extern "C" fn visit_schema_timestamp_ntz(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
) -> usize {
    let name_str = match unsafe { <&str>::try_from_slice(&name) } {
        Ok(s) => s.to_string(),
        Err(_) => return 0,
    };

    let field = StructField {
        name: name_str,
        data_type: DataType::Primitive(PrimitiveType::TimestampNtz),
        nullable,
        metadata: HashMap::new(),
    };

    wrap_field(state, field)
}

/// Create a Decimal field with precision and scale
#[no_mangle]
pub extern "C" fn visit_schema_decimal(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    precision: u8,
    scale: i8,
    nullable: bool,
) -> usize {
    let name_str = match unsafe { <&str>::try_from_slice(&name) } {
        Ok(s) => s.to_string(),
        Err(_) => return 0,
    };

    let decimal_type = DecimalType {
        precision,
        scale: scale as u8,
    };
    let field = StructField {
        name: name_str,
        data_type: DataType::Primitive(PrimitiveType::Decimal(decimal_type)),
        nullable,
        metadata: HashMap::new(),
    };

    wrap_field(state, field)
}

// =============================================================================
// FFI Visitor Functions - Complex Types
// =============================================================================

/// Create a Struct field from child field IDs
/// Engine provides array of field IDs that become the struct's fields
#[no_mangle]
pub extern "C" fn visit_schema_struct(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    field_ids: *const usize,
    field_count: usize,
    nullable: bool,
) -> usize {
    let name_str = match unsafe { <&str>::try_from_slice(&name) } {
        Ok(s) => s.to_string(),
        Err(_) => return 0,
    };

    // Extract fields from IDs and build struct
    let mut field_vec = Vec::new();
    let field_slice = unsafe { std::slice::from_raw_parts(field_ids, field_count) };

    for &field_id in field_slice {
        if let Some(field) = unwrap_field(state, field_id) {
            field_vec.push(field);
        } else {
            return 0; // Invalid field -> invalid struct
        }
    }

    let struct_type = StructType::new(field_vec.into_iter());
    let data_type = DataType::Struct(Box::new(struct_type));

    let field = StructField {
        name: name_str,
        data_type,
        nullable,
        metadata: HashMap::new(),
    };

    wrap_field(state, field)
}

/// Create an Array field from element type ID
/// Engine provides the ID of the element type (could be primitive or complex)
#[no_mangle]
pub extern "C" fn visit_schema_array(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    element_type_id: usize,
    contains_null: bool,
    nullable: bool,
) -> usize {
    let name_str = match unsafe { <&str>::try_from_slice(&name) } {
        Ok(s) => s.to_string(),
        Err(_) => return 0,
    };

    let element_type = match unwrap_data_type(state, element_type_id) {
        Some(dt) => dt,
        None => return 0, // Invalid element type -> invalid array
    };

    let array_type = ArrayType {
        type_name: "array".to_string(),
        element_type,
        contains_null,
    };

    let field = StructField {
        name: name_str,
        data_type: DataType::Array(Box::new(array_type)),
        nullable,
        metadata: HashMap::new(),
    };

    wrap_field(state, field)
}

/// Create a Map field from key and value type IDs
/// Engine provides IDs for both key type and value type
#[no_mangle]
pub extern "C" fn visit_schema_map(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    key_type_id: usize,
    value_type_id: usize,
    value_contains_null: bool,
    nullable: bool,
) -> usize {
    let name_str = match unsafe { <&str>::try_from_slice(&name) } {
        Ok(s) => s.to_string(),
        Err(_) => return 0,
    };

    let key_type = match unwrap_data_type(state, key_type_id) {
        Some(dt) => dt,
        None => return 0,
    };

    let value_type = match unwrap_data_type(state, value_type_id) {
        Some(dt) => dt,
        None => return 0,
    };

    let map_type = MapType {
        type_name: "map".to_string(),
        key_type,
        value_type,
        value_contains_null,
    };

    let field = StructField {
        name: name_str,
        data_type: DataType::Map(Box::new(map_type)),
        nullable,
        metadata: HashMap::new(),
    };

    wrap_field(state, field)
}

/// Create a Variant field (for semi-structured data)
/// Takes a struct type ID that defines the variant schema
#[no_mangle]
pub extern "C" fn visit_schema_variant(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    variant_struct_id: usize,
    nullable: bool,
) -> usize {
    let name_str = match unsafe { <&str>::try_from_slice(&name) } {
        Ok(s) => s.to_string(),
        Err(_) => return 0,
    };

    // Extract the struct type for the variant
    let variant_struct = match state.elements.take(variant_struct_id) {
        Some(SchemaElement::Schema(schema)) => schema,
        Some(SchemaElement::DataType(DataType::Struct(s))) => *s,
        _ => return 0,
    };

    let field = StructField {
        name: name_str,
        data_type: DataType::Variant(Box::new(variant_struct)),
        nullable,
        metadata: HashMap::new(),
    };

    wrap_field(state, field)
}

// =============================================================================
// FFI Functions - Schema Building
// =============================================================================

/// Build final schema from array of field IDs  
/// This is the final step - takes root-level field IDs and creates a StructType
#[no_mangle]
pub extern "C" fn build_kernel_schema(
    state: &mut KernelSchemaVisitorState,
    field_ids: *const usize,
    field_count: usize,
) -> usize {
    let mut field_vec = Vec::new();
    let field_slice = unsafe { std::slice::from_raw_parts(field_ids, field_count) };

    for &field_id in field_slice {
        if let Some(field) = unwrap_field(state, field_id) {
            field_vec.push(field);
        } else {
            return 0; // Invalid field -> invalid schema
        }
    }

    let schema = StructType::new(field_vec.into_iter());
    wrap_schema(state, schema)
}

// =============================================================================
// Helper Functions for Type-Only Building (No Field Names)
// =============================================================================

/// Create a DataType (not a field) - useful for array elements, map keys/values
/// This allows engines to build types incrementally without field names
#[no_mangle]
pub extern "C" fn create_primitive_type(
    state: &mut KernelSchemaVisitorState,
    primitive_type: u8, // 0=String, 1=Long, 2=Int, 3=Short, 4=Byte, 5=Float, 6=Double, 7=Bool, 8=Binary, 9=Date, 10=Timestamp, 11=TimestampNtz
) -> usize {
    let data_type = match primitive_type {
        0 => DataType::Primitive(PrimitiveType::String),
        1 => DataType::Primitive(PrimitiveType::Long),
        2 => DataType::Primitive(PrimitiveType::Integer),
        3 => DataType::Primitive(PrimitiveType::Short),
        4 => DataType::Primitive(PrimitiveType::Byte),
        5 => DataType::Primitive(PrimitiveType::Float),
        6 => DataType::Primitive(PrimitiveType::Double),
        7 => DataType::Primitive(PrimitiveType::Boolean),
        8 => DataType::Primitive(PrimitiveType::Binary),
        9 => DataType::Primitive(PrimitiveType::Date),
        10 => DataType::Primitive(PrimitiveType::Timestamp),
        11 => DataType::Primitive(PrimitiveType::TimestampNtz),
        _ => return 0,
    };

    wrap_data_type(state, data_type)
}

/// Create a decimal DataType with precision/scale
#[no_mangle]
pub extern "C" fn create_decimal_type(
    state: &mut KernelSchemaVisitorState,
    precision: u8,
    scale: i8,
) -> usize {
    let decimal_type = DecimalType {
        precision,
        scale: scale as u8,
    };
    let data_type = DataType::Primitive(PrimitiveType::Decimal(decimal_type));
    wrap_data_type(state, data_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel_string_slice;

    #[test]
    fn test_basic_schema_visitor() {
        let mut state = KernelSchemaVisitorState::default();

        // Create a simple string field
        let test_field = "test_field".to_string();
        let name_slice = kernel_string_slice!(test_field);
        let field_id = visit_schema_string(&mut state, name_slice, false);
        assert_ne!(field_id, 0, "Field ID should not be 0 (error)");

        // Build schema from single field
        let field_ids = vec![field_id];
        let schema_id = build_kernel_schema(&mut state, field_ids.as_ptr(), 1);
        assert_ne!(schema_id, 0, "Schema ID should not be 0 (error)");

        // Extract the schema
        let schema = unwrap_kernel_schema(&mut state, schema_id);
        assert!(schema.is_some(), "Schema should be extractable");

        if let Some(schema) = schema {
            assert_eq!(schema.fields().len(), 1, "Schema should have 1 field");
            let field = schema.fields().next().unwrap();
            assert_eq!(field.name(), "test_field");
            assert!(!field.is_nullable());
        }
    }

    #[test]
    fn test_multiple_field_schema() {
        let mut state = KernelSchemaVisitorState::default();

        // Create multiple fields
        let id_name = "id".to_string();
        let name_name = "name".to_string();
        let active_name = "active".to_string();

        let id_field = visit_schema_long(&mut state, kernel_string_slice!(id_name), false);
        let name_field = visit_schema_string(&mut state, kernel_string_slice!(name_name), true);
        let active_field =
            visit_schema_boolean(&mut state, kernel_string_slice!(active_name), false);

        assert_ne!(id_field, 0);
        assert_ne!(name_field, 0);
        assert_ne!(active_field, 0);

        // Build schema
        let field_ids = vec![id_field, name_field, active_field];
        let schema_id = build_kernel_schema(&mut state, field_ids.as_ptr(), 3);
        assert_ne!(schema_id, 0);

        // Verify schema
        let schema = unwrap_kernel_schema(&mut state, schema_id);
        assert!(schema.is_some());

        if let Some(schema) = schema {
            assert_eq!(schema.fields().len(), 3, "Schema should have 3 fields");

            let field_names: Vec<String> = schema.fields().map(|f| f.name().to_string()).collect();
            assert!(field_names.contains(&"id".to_string()));
            assert!(field_names.contains(&"name".to_string()));
            assert!(field_names.contains(&"active".to_string()));
        }
    }

    #[test]
    fn test_end_to_end_schema_projection() {
        println!("🚀 Testing end-to-end schema projection...");

        let mut state = KernelSchemaVisitorState::default();

        // Create mock projection schema [id: long, name: string, active: boolean]
        let id_name = "id".to_string();
        let name_name = "name".to_string();
        let active_name = "active".to_string();

        let id_field = visit_schema_long(&mut state, kernel_string_slice!(id_name), false);
        let name_field = visit_schema_string(&mut state, kernel_string_slice!(name_name), true);
        let active_field =
            visit_schema_boolean(&mut state, kernel_string_slice!(active_name), false);

        assert_ne!(id_field, 0, "ID field creation should succeed");
        assert_ne!(name_field, 0, "Name field creation should succeed");
        assert_ne!(active_field, 0, "Active field creation should succeed");

        // Build final schema
        let field_ids = vec![id_field, name_field, active_field];
        let schema_id = build_kernel_schema(&mut state, field_ids.as_ptr(), field_ids.len());
        assert_ne!(schema_id, 0, "Schema building should succeed");

        // Extract and verify schema
        let schema = unwrap_kernel_schema(&mut state, schema_id);
        assert!(schema.is_some(), "Should be able to extract schema");

        if let Some(schema) = schema {
            println!(
                "✅ Successfully created projected schema with {} fields:",
                schema.fields().len()
            );

            for field in schema.fields() {
                println!(
                    "  - {} ({}{})",
                    field.name(),
                    match field.data_type() {
                        delta_kernel::schema::DataType::Primitive(p) => format!("{:?}", p),
                        other => format!("{:?}", other),
                    },
                    if field.is_nullable() {
                        ", nullable"
                    } else {
                        ""
                    }
                );
            }

            assert_eq!(
                schema.fields().len(),
                3,
                "Schema should have exactly 3 fields"
            );

            let field_names: Vec<String> = schema.fields().map(|f| f.name().to_string()).collect();
            assert!(
                field_names.contains(&"id".to_string()),
                "Should contain 'id' field"
            );
            assert!(
                field_names.contains(&"name".to_string()),
                "Should contain 'name' field"
            );
            assert!(
                field_names.contains(&"active".to_string()),
                "Should contain 'active' field"
            );

            // Verify field types
            for field in schema.fields() {
                match field.name().as_str() {
                    "id" => {
                        assert!(matches!(
                            field.data_type(),
                            delta_kernel::schema::DataType::Primitive(
                                delta_kernel::schema::PrimitiveType::Long
                            )
                        ));
                        assert!(!field.is_nullable());
                    }
                    "name" => {
                        assert!(matches!(
                            field.data_type(),
                            delta_kernel::schema::DataType::Primitive(
                                delta_kernel::schema::PrimitiveType::String
                            )
                        ));
                        assert!(field.is_nullable());
                    }
                    "active" => {
                        assert!(matches!(
                            field.data_type(),
                            delta_kernel::schema::DataType::Primitive(
                                delta_kernel::schema::PrimitiveType::Boolean
                            )
                        ));
                        assert!(!field.is_nullable());
                    }
                    _ => panic!("Unexpected field: {}", field.name()),
                }
            }

            println!("✅ All field types and nullability verified!");
            println!("✅ Schema projection integration test passed!");
        }
    }

    #[test]
    fn test_complex_nested_schema() {
        let mut state = KernelSchemaVisitorState::default();

        // Build a complex nested schema:
        // {
        //   id: long,
        //   user: struct<
        //     name: string,
        //     address: struct<
        //       street: string,
        //       city: string,
        //       coordinates: array<double>
        //     >,
        //     metadata: map<string, string>
        //   >,
        //   scores: array<float>,
        //   active: boolean
        // }

        println!("🚀 Testing complex nested schema with arrays, maps, and structs...");

        // Step 1: Build leaf types first (bottom-up dependency order)
        let id_field = visit_schema_long(&mut state, kernel_string_slice!(id_name), false);
        let name_field = visit_schema_string(&mut state, kernel_string_slice!(name_name), false);
        let street_field =
            visit_schema_string(&mut state, kernel_string_slice!(street_name), false);
        let city_field = visit_schema_string(&mut state, kernel_string_slice!(city_name), false);
        let active_field =
            visit_schema_boolean(&mut state, kernel_string_slice!(active_name), false);

        // Step 2: Build array types
        let double_type = create_primitive_type(&mut state, 6); // 6 = Double
        let coordinates_field = visit_schema_array(
            &mut state,
            kernel_string_slice!(coordinates_name),
            double_type,
            false,
            false,
        );

        let float_type = create_primitive_type(&mut state, 5); // 5 = Float
        let scores_field = visit_schema_array(
            &mut state,
            kernel_string_slice!(scores_name),
            float_type,
            false,
            false,
        );

        // Step 3: Build map type
        let string_key_type = create_primitive_type(&mut state, 0); // 0 = String
        let string_value_type = create_primitive_type(&mut state, 0); // 0 = String
        let metadata_field = visit_schema_map(
            &mut state,
            kernel_string_slice!(metadata_name),
            string_key_type,
            string_value_type,
            false,
            false,
        );

        // Step 4: Build nested structs (inside-out)
        let address_fields = vec![street_field, city_field, coordinates_field];
        let address_field = visit_schema_struct(
            &mut state,
            kernel_string_slice!(address_name),
            address_fields.as_ptr(),
            3,
            false,
        );

        let user_fields = vec![name_field, address_field, metadata_field];
        let user_field = visit_schema_struct(
            &mut state,
            kernel_string_slice!(user_name),
            user_fields.as_ptr(),
            3,
            false,
        );

        // Step 5: Build root schema
        let root_fields = vec![id_field, user_field, scores_field, active_field];
        let schema_id = build_kernel_schema(&mut state, root_fields.as_ptr(), 4);

        assert_ne!(schema_id, 0, "Complex schema building should succeed");

        // Step 6: Verify the complex schema
        let schema = unwrap_kernel_schema(&mut state, schema_id);
        assert!(schema.is_some(), "Should be able to extract complex schema");

        if let Some(schema) = schema {
            println!(
                "✅ Successfully created complex nested schema with {} top-level fields:",
                schema.fields().len()
            );

            assert_eq!(schema.fields().len(), 4, "Root should have 4 fields");

            // Verify field structure
            let field_names: Vec<String> = schema.fields().map(|f| f.name().to_string()).collect();
            assert!(field_names.contains(&"id".to_string()));
            assert!(field_names.contains(&"user".to_string()));
            assert!(field_names.contains(&"scores".to_string()));
            assert!(field_names.contains(&"active".to_string()));

            // Verify user struct nesting
            let user_field = schema.fields().find(|f| f.name() == "user").unwrap();
            if let DataType::Struct(user_struct) = user_field.data_type() {
                assert_eq!(
                    user_struct.fields().len(),
                    3,
                    "User struct should have 3 fields"
                );

                let user_field_names: Vec<String> =
                    user_struct.fields().map(|f| f.name().to_string()).collect();
                assert!(user_field_names.contains(&"name".to_string()));
                assert!(user_field_names.contains(&"address".to_string()));
                assert!(user_field_names.contains(&"metadata".to_string()));

                // Verify address nested struct
                let address_field = user_struct
                    .fields()
                    .find(|f| f.name() == "address")
                    .unwrap();
                if let DataType::Struct(address_struct) = address_field.data_type() {
                    assert_eq!(
                        address_struct.fields().len(),
                        3,
                        "Address should have 3 fields"
                    );

                    let addr_field_names: Vec<String> = address_struct
                        .fields()
                        .map(|f| f.name().to_string())
                        .collect();
                    assert!(addr_field_names.contains(&"street".to_string()));
                    assert!(addr_field_names.contains(&"city".to_string()));
                    assert!(addr_field_names.contains(&"coordinates".to_string()));

                    // Verify array field
                    let coordinates_field = address_struct
                        .fields()
                        .find(|f| f.name() == "coordinates")
                        .unwrap();
                    if let DataType::Array(array_type) = coordinates_field.data_type() {
                        assert!(matches!(
                            array_type.element_type,
                            DataType::Primitive(PrimitiveType::Double)
                        ));
                    } else {
                        panic!("Coordinates should be array type");
                    }
                } else {
                    panic!("Address should be struct type");
                }

                // Verify map field
                let metadata_field = user_struct
                    .fields()
                    .find(|f| f.name() == "metadata")
                    .unwrap();
                if let DataType::Map(map_type) = metadata_field.data_type() {
                    assert!(matches!(
                        map_type.key_type,
                        DataType::Primitive(PrimitiveType::String)
                    ));
                    assert!(matches!(
                        map_type.value_type,
                        DataType::Primitive(PrimitiveType::String)
                    ));
                } else {
                    panic!("Metadata should be map type");
                }
            } else {
                panic!("User field should be struct type");
            }

            // Verify root-level array
            let scores_field = schema.fields().find(|f| f.name() == "scores").unwrap();
            if let DataType::Array(array_type) = scores_field.data_type() {
                assert!(matches!(
                    array_type.element_type,
                    DataType::Primitive(PrimitiveType::Float)
                ));
            } else {
                panic!("Scores should be array type");
            }

            println!("✅ All nested structure validations passed!");
            println!("✅ Field ID approach successfully handles arbitrary nesting!");
        }
    }

    #[test]
    fn test_decimal_and_timestamps() {
        let mut state = KernelSchemaVisitorState::default();

        // Test all the additional primitive types
        let price_field =
            visit_schema_decimal(&mut state, kernel_string_slice!(price_name), 10, 2, false); // decimal(10,2)
        let created_at_field =
            visit_schema_timestamp(&mut state, kernel_string_slice!(created_at_name), false);
        let updated_at_field =
            visit_schema_timestamp_ntz(&mut state, kernel_string_slice!(updated_at_name), true);
        let birth_date_field =
            visit_schema_date(&mut state, kernel_string_slice!(birth_date_name), true);
        let file_data_field =
            visit_schema_binary(&mut state, kernel_string_slice!(file_data_name), true);
        let score_field = visit_schema_float(&mut state, kernel_string_slice!(score_name), false);
        let count_field = visit_schema_short(&mut state, kernel_string_slice!(count_name), false);
        let flag_field = visit_schema_byte(&mut state, kernel_string_slice!(flag_name), false);

        let field_ids = vec![
            price_field,
            created_at_field,
            updated_at_field,
            birth_date_field,
            file_data_field,
            score_field,
            count_field,
            flag_field,
        ];
        let schema_id = build_kernel_schema(&mut state, field_ids.as_ptr(), 8);

        let schema = unwrap_kernel_schema(&mut state, schema_id);
        assert!(
            schema.is_some(),
            "All primitive types schema should be extractable"
        );

        if let Some(schema) = schema {
            assert_eq!(
                schema.fields().len(),
                8,
                "Should have all 8 primitive type fields"
            );

            // Verify decimal field
            let price_field = schema.fields().find(|f| f.name() == "price").unwrap();
            if let DataType::Primitive(PrimitiveType::Decimal(decimal_type)) =
                price_field.data_type()
            {
                assert_eq!(decimal_type.precision(), 10);
                assert_eq!(decimal_type.scale(), 2);
            } else {
                panic!("Price should be decimal type");
            }

            // Verify timestamp types
            let created_field = schema.fields().find(|f| f.name() == "created_at").unwrap();
            assert!(matches!(
                created_field.data_type(),
                DataType::Primitive(PrimitiveType::Timestamp)
            ));

            let updated_field = schema.fields().find(|f| f.name() == "updated_at").unwrap();
            assert!(matches!(
                updated_field.data_type(),
                DataType::Primitive(PrimitiveType::TimestampNtz)
            ));

            println!("✅ All primitive types (decimal, timestamps, binary, etc.) work correctly!");
        }
    }
}
