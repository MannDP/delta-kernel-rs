/*
 * End-to-End Example: Engine Schema Projection 
 * 
 * This demonstrates how an engine (e.g. DuckDB) can use the simplified EngineSchema
 * FFI to specify column projections for pushdown optimization.
 * 
 * The API is now MUCH simpler - no field IDs, no start/end calls!
 * Engine just describes what columns it wants, kernel builds the schema.
 * 
 * Example Scenario: Engine wants to project only columns ["id", "name", "active"]
 * from a table with schema [id: long, name: string, age: integer, active: boolean, score: double]
 */

#include "delta_kernel_ffi.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// =============================================================================
// Mock Engine Schema Representation
// =============================================================================

typedef enum {
    ENGINE_TYPE_LONG,
    ENGINE_TYPE_STRING, 
    ENGINE_TYPE_INTEGER,
    ENGINE_TYPE_BOOLEAN,
    ENGINE_TYPE_DOUBLE,
    ENGINE_TYPE_STRUCT,
} EngineDataType;

typedef struct {
    char* name;
    EngineDataType type;
    bool nullable;
} EngineField;

typedef struct {
    EngineField* fields;
    size_t field_count;
} EngineSchemaData;

// =============================================================================
// Engine's Schema Visitor Implementation  
// =============================================================================

// **NEW SIMPLIFIED API**: Engine just describes what it wants, kernel builds it!
// No field IDs, no complex management - just describe your projection!
extern "C" void engine_schema_visitor(
    void* schema_ptr,
    KernelSchemaVisitorState* state
) {
    EngineSchemaData* engine_schema = (EngineSchemaData*)schema_ptr;
    printf("Engine visitor called with %zu fields\n", engine_schema->field_count);
    
    // Simply describe each field - kernel handles all the building!
    for (size_t i = 0; i < engine_schema->field_count; i++) {
        EngineField* field = &engine_schema->fields[i];
        
        // Create KernelStringSlice for field name
        KernelStringSlice name_slice = {
            .ptr = field->name,
            .len = strlen(field->name)
        };
        
        // Just call the appropriate visitor - no IDs to track!
        switch (field->type) {
            case ENGINE_TYPE_LONG:
                visit_schema_long(state, name_slice, field->nullable);
                printf("  Added LONG field '%s'\n", field->name);
                break;
                
            case ENGINE_TYPE_STRING:
                visit_schema_string(state, name_slice, field->nullable);
                printf("  Added STRING field '%s'\n", field->name);
                break;
                
            case ENGINE_TYPE_INTEGER:
                visit_schema_integer(state, name_slice, field->nullable);
                printf("  Added INTEGER field '%s'\n", field->name);
                break;
                
            case ENGINE_TYPE_BOOLEAN:
                visit_schema_boolean(state, name_slice, field->nullable);
                printf("  Added BOOLEAN field '%s'\n", field->name);
                break;
                
            case ENGINE_TYPE_DOUBLE:
                visit_schema_double(state, name_slice, field->nullable);
                printf("  Added DOUBLE field '%s'\n", field->name);
                break;
                
            default:
                printf("  Unsupported engine type for field '%s'\n", field->name);
                continue;
        }
    }
    
    // That's it! Kernel automatically builds the final schema when needed.
    // No manual building, no field ID management, no complex state tracking.
    printf("Schema description complete - kernel will build final schema\n");
}

// =============================================================================
// Example Usage Function
// =============================================================================

void demonstrate_schema_projection() {
    printf("=== Schema Projection Example ===\n");
    
    // **Example Scenario**: Engine wants to project ["id", "name", "active"] 
    // from a larger table schema
    EngineField projection_fields[] = {
        {"id", ENGINE_TYPE_LONG, false},        // id: long not null  
        {"name", ENGINE_TYPE_STRING, true},     // name: string nullable
        {"active", ENGINE_TYPE_BOOLEAN, false}  // active: boolean not null
    };
    
    EngineSchemaData engine_projection = {
        .fields = projection_fields,
        .field_count = 3
    };
    
    // Create EngineSchema FFI structure
    EngineSchema projection = {
        .schema = &engine_projection,
        .visitor = engine_schema_visitor
    };
    
    printf("Created EngineSchema with %zu projected columns:\n", engine_projection.field_count);
    for (size_t i = 0; i < engine_projection.field_count; i++) {
        printf("  - %s (%s%s)\n", 
               projection_fields[i].name,
               projection_fields[i].type == ENGINE_TYPE_LONG ? "long" :
               projection_fields[i].type == ENGINE_TYPE_STRING ? "string" :
               projection_fields[i].type == ENGINE_TYPE_BOOLEAN ? "boolean" : "unknown",
               projection_fields[i].nullable ? ", nullable" : "");
    }
    
    // **Design Decision**: In real usage, this would be passed to scan() function:
    // 
    // SharedScan* scan_result = scan(
    //     snapshot_handle,
    //     engine_handle, 
    //     NULL,              // no predicate
    //     &projection        // our projection schema
    // );
    // 
    // The kernel would then:
    // 1. Call engine_schema_visitor with projection.schema and a new KernelSchemaVisitorState
    // 2. The visitor builds kernel schema incrementally via visitor functions
    // 3. Kernel extracts final Schema and applies it to ScanBuilder.with_schema()
    // 4. Only the projected columns get read from parquet files
    
    printf("\nProjection schema successfully created!\n");
    printf("This would enable column pruning during scan for significant I/O savings.\n");
}

// =============================================================================
// Main Function  
// =============================================================================

int main() {
    printf("Delta Kernel FFI Schema Projection Demo\n");
    printf("======================================\n\n");
    
    demonstrate_schema_projection();
    
    printf("\n=== Key Design Decisions ===\n");
    printf("1. **Extremely Simple API**: Just describe fields, kernel does the rest!\n");
    printf("2. **No Field IDs**: Engine doesn't track anything - just calls visitors\n");  
    printf("3. **No Start/End Complexity**: Engine describes, kernel builds eagerly\n");
    printf("4. **Direct field addition**: Each visitor call immediately adds field\n");
    printf("5. **Flat schema focus**: Perfect for 99%% of projection use cases\n");
    printf("6. **Memory safety**: Engine and kernel still own their respective objects\n");
    
    return 0;
}