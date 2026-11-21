use crate::expression_visitor::{rewrite_expressions, Context};
use feldera_sql_expr::ErrorSites;
use feldera_sql_ir::{Circuit, Error};

pub fn create_runtime_error_wrappers(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let mut sites = ErrorSites::default();
    let mut active = None;
    rewrite_expressions(circuit, next, |editor, root, context, _| {
        if let Context::Operator(id) = context {
            if active != Some(id) {
                sites = ErrorSites::for_operator();
                active = Some(id);
            }
        }
        sites.wrap(editor, root)
    })
}
