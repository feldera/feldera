use crate::{
    codegen::{
        utils::FunctionBuilderExt, CodegenCtx, VTable, TRAP_ABORT, TRAP_ASSERT_EQ,
        TRAP_CAPACITY_OVERFLOW,
    },
    ir::{exprs::Call, ColumnType, ExprId},
    ThinStr,
};
use cranelift::prelude::{types, FunctionBuilder, InstBuilder, IntCC, MemFlags};
use std::mem::align_of;

impl CodegenCtx<'_> {
    pub(super) fn call(&mut self, expr_id: ExprId, call: &Call, builder: &mut FunctionBuilder<'_>) {
        match call.function() {
            "dbsp.error.abort" => self.error_abort(builder),

            // fn(*mut {vec_ptr, row_vtable}, row_value)
            "dbsp.row.vec.push" => self.row_vec_push(call, builder),

            // `fn(string: str, len: usize)` (mutates the given string)
            "dbsp.str.truncate" => self.string_truncate(call, builder),

            // `fn(string: str, len: usize) -> str`
            "dbsp.str.truncate_clone" => self.string_truncate_clone(expr_id, call, builder),

            // `fn(first: str, second: str)` (mutates the first string)
            "dbsp.str.concat" => self.string_concat(call, builder),

            // `fn(first: str, second: str) -> str`
            "dbsp.str.concat_clone" => self.string_concat_clone(expr_id, call, builder),

            // `fn(string: str)` (mutates the given string)
            "dbsp.str.clear" => self.string_clear(call, builder),

            "dbsp.str.bit_length" => self.string_bit_length(expr_id, call, builder),
            "dbsp.str.char_length" => self.string_char_length(expr_id, call, builder),
            "dbsp.str.byte_length" => self.string_byte_length(expr_id, call, builder),

            // `fn(timestamp) -> date
            "dbsp.timestamp.to_date" => self.timestamp_to_date(expr_id, call, builder),

            // `fn(timestamp) -> i64`
            "dbsp.timestamp.epoch" => self.timestamp_epoch(expr_id, call, builder),

            // `fn(timestamp) -> i64`
            "dbsp.timestamp.year" => {
                let millis = self.value(call.args()[0]);
                let year = self.timestamp_year(millis, builder);
                self.add_expr(expr_id, year, ColumnType::I64, None);
            }

            // TODO: Implement these all natively, they're all simple functions
            // and function dispatch is a heavy price (this is especially important
            // for frequently-used functions like `@dbsp.timestamp.year()`)
            "dbsp.timestamp.month" => self.timestamp_month(expr_id, call, builder),
            "dbsp.timestamp.day" => self.timestamp_day(expr_id, call, builder),
            "dbsp.timestamp.quarter" => self.timestamp_quarter(expr_id, call, builder),
            "dbsp.timestamp.decade" => self.timestamp_decade(expr_id, call, builder),
            "dbsp.timestamp.century" => self.timestamp_century(expr_id, call, builder),
            "dbsp.timestamp.millennium" => self.timestamp_millennium(expr_id, call, builder),
            "dbsp.timestamp.iso_year" => self.timestamp_iso_year(expr_id, call, builder),
            "dbsp.timestamp.week" => self.timestamp_week(expr_id, call, builder),
            "dbsp.timestamp.day_of_week" => self.timestamp_day_of_week(expr_id, call, builder),
            "dbsp.timestamp.iso_day_of_week" => {
                self.timestamp_iso_day_of_week(expr_id, call, builder);
            }
            "dbsp.timestamp.day_of_year" => self.timestamp_day_of_year(expr_id, call, builder),
            "dbsp.timestamp.millisecond" => self.timestamp_millisecond(expr_id, call, builder),
            "dbsp.timestamp.microsecond" => self.timestamp_microsecond(expr_id, call, builder),
            "dbsp.timestamp.second" => self.timestamp_second(expr_id, call, builder),
            "dbsp.timestamp.minute" => self.timestamp_minute(expr_id, call, builder),
            "dbsp.timestamp.hour" => self.timestamp_hour(expr_id, call, builder),
            "dbsp.timestamp.floor_week" => self.timestamp_floor_week(expr_id, call, builder),

            // `fn(date) -> timestamp
            "dbsp.date.to_timestamp" => self.date_to_timestamp(expr_id, call, builder),

            // `fn(date) -> i32`
            "dbsp.date.epoch" => self.date_epoch(expr_id, call, builder),

            // TODO: Implement these all natively, they're all simple functions
            // and function dispatch is a heavy price (this is especially important
            // for frequently-used functions like `@dbsp.date.year()`)
            "dbsp.date.year" => self.date_year(expr_id, call, builder),
            "dbsp.date.month" => self.date_month(expr_id, call, builder),
            "dbsp.date.day" => self.date_day(expr_id, call, builder),
            "dbsp.date.quarter" => self.date_quarter(expr_id, call, builder),
            "dbsp.date.decade" => self.date_decade(expr_id, call, builder),
            "dbsp.date.century" => self.date_century(expr_id, call, builder),
            "dbsp.date.millennium" => self.date_millennium(expr_id, call, builder),
            "dbsp.date.iso_year" => self.date_iso_year(expr_id, call, builder),
            "dbsp.date.week" => self.date_week(expr_id, call, builder),
            "dbsp.date.day_of_week" => self.date_day_of_week(expr_id, call, builder),
            "dbsp.date.iso_day_of_week" => self.date_iso_day_of_week(expr_id, call, builder),
            "dbsp.date.day_of_year" => self.date_day_of_year(expr_id, call, builder),

            // Date functions that always return zero
            function @ ("dbsp.date.hour"
            | "dbsp.date.minute"
            | "dbsp.date.second"
            | "dbsp.date.millisecond"
            | "dbsp.date.microsecond") => {
                self.constant_zero_date_function(expr_id, call, function, builder);
            }

            unknown => todo!("unknown function call: @{unknown}"),
        }
    }

    fn error_abort(&self, builder: &mut FunctionBuilder<'_>) {
        let trap = builder.ins().trap(TRAP_ABORT);

        if let Some(writer) = self.comment_writer.as_deref() {
            writer
                .borrow_mut()
                .add_comment(trap, "call @dbsp.error.abort()");
        }
    }

    fn row_vec_push(&mut self, call: &Call, builder: &mut FunctionBuilder<'_>) {
        let vec_layout = self.layout_cache.row_layout_cache().row_vector();
        let native_vec_layout = self.layout_cache.layout_of(vec_layout);

        debug_assert_eq!(call.args().len(), 2);
        debug_assert_eq!(call.arg_types().len(), 2);
        debug_assert_eq!(call.arg_types()[0].as_row(), Some(vec_layout));
        let row_layout = self.expr_layouts[&call.args()[1]];
        debug_assert_eq!(call.arg_types()[1].as_row(), Some(row_layout));

        let (vec, row) = (self.value(call.args()[0]), self.value(call.args()[1]));

        let ptr_ty = self.pointer_type();
        let flags = MemFlags::trusted();
        let vec_ptr = builder
            .ins()
            .load(ptr_ty, flags, vec, native_vec_layout.offset_of(0) as i32);
        let vtable_ptr =
            builder
                .ins()
                .load(ptr_ty, flags, vec, native_vec_layout.offset_of(1) as i32);

        if self.debug_assertions() {
            // Assert that the vec pointer is valid
            self.assert_ptr_valid(vec_ptr, align_of::<Vec<crate::row::Row>>() as u32, builder);

            // Assert that the vtable pointer is valid
            self.assert_ptr_valid(vtable_ptr, align_of::<VTable>() as u32, builder);

            // Assert that the row pointer is valid
            let row_align = self.layout_cache.layout_of(row_layout).align();
            self.assert_ptr_valid(row, row_align, builder);

            // Assert that the vtable is associated with the correct layout
            let vtable_layout = builder.ins().load(
                types::I32,
                MemFlags::trusted(),
                vtable_ptr,
                VTable::layout_id_offset() as i32,
            );
            let expected_layout = builder
                .ins()
                .iconst(types::I32, row_layout.into_inner() as i64);
            let are_equal = builder
                .ins()
                .icmp(IntCC::Equal, vtable_layout, expected_layout);
            builder.ins().trapz(are_equal, TRAP_ASSERT_EQ);
        }

        let dbsp_row_vec_push = self.imports.row_vec_push(self.module, builder.func);
        let call_inst = builder
            .ins()
            .call(dbsp_row_vec_push, &[vec_ptr, vtable_ptr, row]);

        if let Some(writer) = self.comment_writer.as_deref() {
            let layout = self.layout_cache.layout_of(row_layout);
            writer
                .borrow_mut()
                .add_comment(call_inst, format!("call @dbsp.row.vec.push() for {layout}"));
        }
    }

    fn string_truncate(&mut self, call: &Call, builder: &mut FunctionBuilder<'_>) {
        let (string, truncated_length) = (self.value(call.args()[0]), self.value(call.args()[1]));

        // Right now with the current design of strings (having them be a
        // pointer to { length, capacity, ..data } instead of { length, capacity, *data
        // }) we have to branch when setting the string's length since `ThinStr`
        // has a sigil value for empty strings that we can't mutate
        let after_block = builder.create_block();
        let set_string_length = builder.create_block();

        // If `string` is the sigil string, skip setting the string's length
        // (the sigil string's length of zero will always be less than or
        // equal to `truncated_length` so the behavior is identical, we
        // just can't mutate the sigil string)
        // TODO: Should we branch on `length == 0` instead of `string == sigil`?
        let is_sigil_string =
            builder
                .ins()
                .icmp_imm(IntCC::Equal, string, ThinStr::sigil_addr() as i64);
        builder
            .ins()
            .brif(is_sigil_string, after_block, &[], set_string_length, &[]);

        if let Some(writer) = self.comment_writer.as_deref() {
            let inst = builder.func.dfg.value_def(is_sigil_string).unwrap_inst();
            writer.borrow_mut().add_comment(
                inst,
                format!("call @dbsp.str.truncate({string}, {truncated_length})"),
            );
        }

        // Build the block where we set the string's truncated length
        builder.switch_to_block(set_string_length);

        if self.debug_assertions() {
            // Assert that the string pointer is a valid pointer
            self.assert_ptr_valid(string, align_of::<ThinStr>() as u32, builder);

            // Assert that `truncated_length` is less than or equal to `isize::MAX`
            let less_than_max = builder.ins().icmp_imm(
                IntCC::UnsignedLessThanOrEqual,
                truncated_length,
                isize::MAX as i64,
            );
            builder.ins().trapz(less_than_max, TRAP_ASSERT_EQ);
        }

        // Get the current length of the string
        let current_length = self.string_length(string, false, builder);

        // The string's length will be set to the minimum of the proposed
        // length and the string's current length
        let new_length = builder.ins().umin(truncated_length, current_length);

        // Ensure that the chosen length is less than the string's capacity
        if self.debug_assertions() {
            let capacity = self.string_capacity(string, false, builder);
            let length_less_than_capacity =
                builder
                    .ins()
                    .icmp(IntCC::UnsignedLessThanOrEqual, new_length, capacity);

            builder
                .ins()
                .trapz(length_less_than_capacity, TRAP_ASSERT_EQ);
        }

        // Get the offset of the length field
        let length_offset = ThinStr::length_offset();

        // Store the new length to the length field
        builder.ins().store(
            MemFlags::trusted(),
            new_length,
            string,
            length_offset as i32,
        );

        // Jump & switch to the after block for the following code
        builder.ins().jump(after_block, &[]);
        builder.switch_to_block(after_block);
        builder.seal_block(set_string_length);
    }

    fn string_truncate_clone(
        &mut self,
        expr_id: ExprId,
        call: &Call,
        builder: &mut FunctionBuilder<'_>,
    ) {
        let (string, truncated_length) = (self.value(call.args()[0]), self.value(call.args()[1]));

        // Right now with the current design of strings (having them be a
        // pointer to { length, capacity, ..data } instead of { length, capacity, *data
        // }) we have to branch when setting the string's length since `ThinStr`
        // has a sigil value for empty strings that we can't mutate
        let after_block = builder.create_block();
        builder.append_block_param(after_block, self.pointer_type());

        let allocate_truncated_string = builder.create_block();

        // Get the current length of the string
        let current_length = self.string_length(string, false, builder);
        // The string's length will be set to the minimum of the proposed
        // length and the string's current length
        let new_length = builder.ins().umin(truncated_length, current_length);

        // If `new_length` is zero, we can directly return an empty string
        let new_length_is_zero = builder.ins().icmp_imm(IntCC::Equal, new_length, 0);

        // If `string` is the sigil string, skip setting the string's length
        // (the sigil string's length of zero will always be less than or
        // equal to `truncated_length` so the behavior is identical, we
        // just can't mutate the sigil string)
        // TODO: Should we branch on `length == 0` instead of `string == sigil`?
        let empty_string = builder
            .ins()
            .iconst(self.pointer_type(), ThinStr::sigil_addr() as i64);
        let is_sigil_string = builder.ins().icmp(IntCC::Equal, string, empty_string);

        let should_return_empty = builder.ins().bor(new_length_is_zero, is_sigil_string);
        builder.ins().brif(
            should_return_empty,
            // If we skip allocating a truncated string, we can directly return an empty string
            after_block,
            &[empty_string],
            allocate_truncated_string,
            &[],
        );

        if let Some(writer) = self.comment_writer.as_deref() {
            let inst = builder.func.dfg.value_def(is_sigil_string).unwrap_inst();
            writer.borrow_mut().add_comment(
                inst,
                format!("call @dbsp.str.truncate({string}, {truncated_length})"),
            );
        }

        // Build the block where we set the string's truncated length
        builder.switch_to_block(allocate_truncated_string);

        // Allocate a string with the given capacity
        let string_with_capacity = self.imports.string_with_capacity(self.module, builder.func);
        let allocated = builder.call_fn(string_with_capacity, &[new_length]);

        // Ensure that neither `string` nor `allocated` are empty strings
        if self.debug_assertions() {
            let string_is_empty = builder.ins().icmp(IntCC::Equal, string, empty_string);
            builder.ins().trapnz(string_is_empty, TRAP_ASSERT_EQ);

            let allocated_is_empty = builder.ins().icmp(IntCC::Equal, allocated, empty_string);
            builder.ins().trapnz(allocated_is_empty, TRAP_ASSERT_EQ);
        }

        // Copy over the data into the allocated string
        let allocated_ptr = self.string_ptr(allocated, builder);
        builder.call_memmove(self.frontend_config(), allocated_ptr, string, new_length);

        // Get the offset of the length field
        let length_offset = ThinStr::length_offset();

        // Store the new length to the length field
        builder.ins().store(
            MemFlags::trusted(),
            new_length,
            allocated,
            length_offset as i32,
        );

        // Jump & switch to the after block for the following code
        builder.ins().jump(after_block, &[allocated]);
        builder.switch_to_block(after_block);
        builder.seal_block(allocate_truncated_string);

        // Get the cloned string
        let cloned = builder.block_params(after_block)[0];
        self.add_expr(expr_id, cloned, ColumnType::String, None);
    }

    fn string_clear(&mut self, call: &Call, builder: &mut FunctionBuilder<'_>) {
        let string = self.value(call.args()[0]);

        // Right now with the current design of strings (having them be a
        // pointer to { length, capacity, ..data } instead of { length, capacity, *data
        // }) we have to branch when setting the string's length since `ThinStr`
        // has a sigil value for empty strings that we can't mutate
        let after_block = builder.create_block();
        let set_string_length = builder.create_block();

        // If `string` is the sigil string, skip setting the string's length
        // since we can't mutate the sigil string and its length is already zero
        let string_capacity = builder.ins().load(
            self.pointer_type(),
            MemFlags::trusted(),
            string,
            ThinStr::capacity_offset() as i32,
        );
        let is_capacity_zero = builder.ins().icmp_imm(IntCC::Equal, string_capacity, 0);

        builder
            .ins()
            .brif(is_capacity_zero, after_block, &[], set_string_length, &[]);

        if let Some(writer) = self.comment_writer.as_deref() {
            let inst = builder.func.dfg.value_def(string_capacity).unwrap_inst();
            writer
                .borrow_mut()
                .add_comment(inst, format!("call @dbsp.str.clear({string})"));
        }

        // Build the block where we set the string's length to zero
        builder.switch_to_block(set_string_length);

        // Assert that the string pointer is a valid pointer
        self.debug_assert_ptr_valid(string, align_of::<ThinStr>() as u32, builder);

        // The string's length will be set to zero
        let new_length = builder.ins().iconst(self.pointer_type(), 0);

        // Get the offset of the length field
        let length_offset = ThinStr::length_offset();

        // Store the new length to the length field
        builder.ins().store(
            MemFlags::trusted(),
            new_length,
            string,
            length_offset as i32,
        );

        // Jump & switch to the after block for the following code
        builder.ins().jump(after_block, &[]);
        builder.switch_to_block(after_block);
        builder.seal_block(set_string_length);
    }

    fn string_concat(&mut self, call: &Call, builder: &mut FunctionBuilder<'_>) {
        let (target, string) = (self.value(call.args()[0]), self.value(call.args()[1]));

        // TODO: Apply readonly where applicable
        let (string_ptr, string_len) = (
            self.string_ptr(string, builder),
            self.string_length(string, false, builder),
        );

        let push_str = self.imports.string_push_str(self.module, builder.func);
        let concat = builder
            .ins()
            .call(push_str, &[target, string_ptr, string_len]);

        if let Some(writer) = self.comment_writer.as_deref() {
            writer
                .borrow_mut()
                .add_comment(concat, format!("call @dbsp.str.concat({target}, {string})"));
        }
    }

    // FIXME: Handle the case where both strings are empty, we can't write to a
    // sigil string
    fn string_concat_clone(
        &mut self,
        expr_id: ExprId,
        call: &Call,
        builder: &mut FunctionBuilder<'_>,
    ) {
        let (first, second) = (self.value(call.args()[0]), self.value(call.args()[1]));

        // Load the string's lengths
        // TODO: Apply readonly where possible
        let (first_length, second_length) = (
            self.string_length(first, false, builder),
            self.string_length(second, false, builder),
        );

        // Add both string's lengths so that we can allocate a string to hold
        // both of them, trapping if it overflows
        let total_length =
            builder
                .ins()
                .uadd_overflow_trap(first_length, second_length, TRAP_CAPACITY_OVERFLOW);

        // Allocate a string of the requested capacity
        let with_capacity = self.imports.string_with_capacity(self.module, builder.func);
        let allocated = builder.call_fn(with_capacity, &[total_length]);
        let allocated_ptr = self.string_ptr(allocated, builder);

        // Copy the first string into the allocation
        let first_ptr = self.string_ptr(first, builder);
        builder.call_memmove(
            self.frontend_config(),
            allocated_ptr,
            first_ptr,
            first_length,
        );

        // Offset the allocation's pointer to the end of the first string's data
        let allocated_ptr2 = builder.ins().iadd(allocated_ptr, first_length);

        // Copy the second string's data into the allocation
        let second_ptr = self.string_ptr(second, builder);
        builder.call_memmove(
            self.frontend_config(),
            allocated_ptr2,
            second_ptr,
            second_length,
        );

        // Set the allocated string's length
        let length_offset = ThinStr::length_offset();
        builder.ins().store(
            MemFlags::trusted(),
            total_length,
            allocated,
            length_offset as i32,
        );

        self.add_expr(expr_id, allocated, ColumnType::String, None);

        if let Some(writer) = self.comment_writer.as_deref() {
            let inst = builder.func.dfg.value_def(allocated).unwrap_inst();
            writer.borrow_mut().add_comment(
                inst,
                format!("call @dbsp.str.concat_clone({first}, {second})"),
            );
        }
    }

    fn string_bit_length(
        &mut self,
        expr_id: ExprId,
        call: &Call,
        builder: &mut FunctionBuilder<'_>,
    ) {
        let string = self.value(call.args()[0]);

        // TODO: Apply readonly when possible
        let length = self.string_length(string, false, builder);
        let bits = builder.ins().imul_imm(length, 8);
        self.add_expr(expr_id, bits, ColumnType::Usize, None);
    }

    fn string_char_length(
        &mut self,
        expr_id: ExprId,
        call: &Call,
        builder: &mut FunctionBuilder<'_>,
    ) {
        let string = self.value(call.args()[0]);

        let ptr = self.string_ptr(string, builder);
        // TODO: Apply readonly when possible
        let length = self.string_length(string, false, builder);

        let count_chars = self.imports.string_count_chars(self.module, builder.func);
        let chars = builder.call_fn(count_chars, &[ptr, length]);

        self.add_expr(expr_id, chars, ColumnType::Usize, None);
    }

    fn string_byte_length(
        &mut self,
        expr_id: ExprId,
        call: &Call,
        builder: &mut FunctionBuilder<'_>,
    ) {
        let string = self.value(call.args()[0]);

        // TODO: Apply readonly when possible
        let length = self.string_length(string, false, builder);
        self.add_expr(expr_id, length, ColumnType::Usize, None);
    }

    fn timestamp_epoch(&mut self, expr_id: ExprId, call: &Call, builder: &mut FunctionBuilder<'_>) {
        let timestamp = self.value(call.args()[0]);

        // timestamp.milliseconds() / 1000
        let epoch = builder.ins().sdiv_imm(timestamp, 1000);
        self.add_expr(expr_id, epoch, ColumnType::I64, None);

        if let Some(writer) = self.comment_writer.as_deref() {
            let inst = builder.func.dfg.value_def(epoch).unwrap_inst();
            writer
                .borrow_mut()
                .add_comment(inst, format!("call @dbsp.timestamp.epoch({timestamp})"));
        }
    }

    fn timestamp_floor_week(
        &mut self,
        expr_id: ExprId,
        call: &Call,
        builder: &mut FunctionBuilder<'_>,
    ) {
        let timestamp = self.value(call.args()[0]);

        let intrinsic = self.imports.timestamp_floor_week(self.module, builder.func);
        let value = builder.call_fn(intrinsic, &[timestamp]);
        self.add_expr(expr_id, value, ColumnType::Timestamp, None);

        if let Some(writer) = self.comment_writer.as_deref() {
            let inst = builder.func.dfg.value_def(value).unwrap_inst();
            writer.borrow_mut().add_comment(
                inst,
                format!("call @dbsp.timestamp.floor_week({timestamp})"),
            );
        }
    }

    fn timestamp_to_date(
        &mut self,
        expr_id: ExprId,
        call: &Call,
        builder: &mut FunctionBuilder<'_>,
    ) {
        let timestamp = self.value(call.args()[0]);

        // (timestamp.milliseconds() / (86400 * 1000)) as i32
        let days = builder.ins().sdiv_imm(timestamp, 86400 * 1000);
        let days_i32 = builder.ins().ireduce(types::I32, days);
        self.add_expr(expr_id, days_i32, ColumnType::I32, None);

        if let Some(writer) = self.comment_writer.as_deref() {
            let inst = builder.func.dfg.value_def(days).unwrap_inst();
            writer
                .borrow_mut()
                .add_comment(inst, format!("call @dbsp.timestamp.to_date({timestamp})"));
        }
    }

    fn date_to_timestamp(
        &mut self,
        expr_id: ExprId,
        call: &Call,
        builder: &mut FunctionBuilder<'_>,
    ) {
        let date = self.value(call.args()[0]);

        // date.days() as i64 * 86400 * 1000
        let days = builder.ins().uextend(types::I64, date);
        let milliseconds = builder.ins().imul_imm(days, 86400 * 1000);
        self.add_expr(expr_id, milliseconds, ColumnType::Timestamp, None);

        if let Some(writer) = self.comment_writer.as_deref() {
            let inst = builder.func.dfg.value_def(days).unwrap_inst();
            writer
                .borrow_mut()
                .add_comment(inst, format!("call @dbsp.date.to_timestamp({date})"));
        }
    }

    fn date_epoch(&mut self, expr_id: ExprId, call: &Call, builder: &mut FunctionBuilder<'_>) {
        let date = self.value(call.args()[0]);

        // date.days() as i64 * 86400
        // TODO: The real code uses an i64 but we keep it as an i32 here
        // let date = builder.ins().uextend(types::I64, date);
        let epoch = builder.ins().imul_imm(date, 86400);
        self.add_expr(expr_id, epoch, ColumnType::I32, None);

        if let Some(writer) = self.comment_writer.as_deref() {
            let inst = builder.func.dfg.value_def(epoch).unwrap_inst();
            writer
                .borrow_mut()
                .add_comment(inst, format!("call @dbsp.date.epoch({date})"));
        }
    }

    fn constant_zero_date_function(
        &mut self,
        expr_id: ExprId,
        call: &Call,
        function: &str,
        builder: &mut FunctionBuilder<'_>,
    ) {
        let zero = builder.ins().iconst(types::I32, 0);
        self.add_expr(expr_id, zero, ColumnType::I32, None);

        if let Some(writer) = self.comment_writer.as_deref() {
            let inst = builder.func.dfg.value_def(zero).unwrap_inst();
            writer.borrow_mut().add_comment(
                inst,
                format!("call @{function}({})", self.value(call.args()[0])),
            );
        }
    }
}

macro_rules! timestamp_intrinsics {
    ($($name:ident),+ $(,)?) => {
        impl CodegenCtx<'_> {
            paste::paste! {
                $(
                    fn [<timestamp_ $name>](&mut self, expr_id: ExprId, call: &Call, builder: &mut FunctionBuilder<'_>) {
                        let timestamp = self.value(call.args()[0]);

                        let intrinsic = self
                            .imports
                            .[<timestamp_ $name>](self.module, builder.func);
                        let value = builder.call_fn(intrinsic, &[timestamp]);
                        self.add_expr(expr_id, value, ColumnType::I64, None);

                        if let Some(writer) = self.comment_writer.as_deref() {
                            let inst = builder.func.dfg.value_def(value).unwrap_inst();
                            writer.borrow_mut().add_comment(
                                inst,
                                format!("call @dbsp.timestamp.{}({timestamp})", stringify!($name)),
                            );
                        }
                    }
                )+
            }
        }
    };
}

timestamp_intrinsics! {
    // year,
    month,
    day,
    quarter,
    decade,
    century,
    millennium,
    iso_year,
    week,
    day_of_week,
    iso_day_of_week,
    day_of_year,
    millisecond,
    microsecond,
    second,
    minute,
    hour,
}

macro_rules! date_intrinsics {
    ($($name:ident),+ $(,)?) => {
        impl CodegenCtx<'_> {
            paste::paste! {
                $(
                    fn [<date_ $name>](&mut self, expr_id: ExprId, call: &Call, builder: &mut FunctionBuilder<'_>) {
                        let date = self.value(call.args()[0]);

                        let intrinsic = self
                            .imports
                            .[<date_ $name>](self.module, builder.func);
                        let value = builder.call_fn(intrinsic, &[date]);
                        self.add_expr(expr_id, value, ColumnType::I64, None);

                        if let Some(writer) = self.comment_writer.as_deref() {
                            let inst = builder.func.dfg.value_def(value).unwrap_inst();
                            writer.borrow_mut().add_comment(
                                inst,
                                format!("call @dbsp.date.{}({date})", stringify!($name)),
                            );
                        }
                    }
                )+
            }
        }
    };
}

date_intrinsics! {
    day,
    week,
    month,
    quarter,
    year,
    decade,
    century,
    millennium,
    iso_year,
    day_of_week,
    iso_day_of_week,
    day_of_year,
}
