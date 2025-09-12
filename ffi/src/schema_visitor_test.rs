//! Simple test for the schema visitor

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel_string_slice;

    #[test]
    fn test_basic_schema_visitor() {
        let mut state = KernelSchemaVisitorState::default();
        
        // Test creating a simple string field
        let name_slice = kernel_string_slice!("test_field");
        let field_id = visit_schema_string(&mut state, name_slice, false);
        assert_ne!(field_id, 0, "Field ID should not be 0 (error)");
        
        // Test building schema from single field
        let field_ids = vec![field_id];
        let schema_id = build_kernel_schema(&mut state, field_ids.as_ptr(), 1);
        assert_ne!(schema_id, 0, "Schema ID should not be 0 (error)");
        
        // Test unwrapping the schema
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
        let id_field = visit_schema_long(&mut state, kernel_string_slice!("id"), false);
        let name_field = visit_schema_string(&mut state, kernel_string_slice!("name"), true);
        let active_field = visit_schema_boolean(&mut state, kernel_string_slice!("active"), false);
        
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
            
            let field_names: Vec<&str> = schema.fields().map(|f| f.name()).collect();
            assert!(field_names.contains(&"id"));
            assert!(field_names.contains(&"name"));
            assert!(field_names.contains(&"active"));
        }
    }
}