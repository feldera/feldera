//! Run the fixed-point scheduler against the live compiler's observed pass.
use feldera_sql_opt::{repeat, RepeatResult};
use jni::{
    errors::ThrowRuntimeExAndDefault,
    jni_sig, jni_str,
    objects::{JClass, JObject},
    EnvUnowned, JValue,
};

// SAFETY: the symbol and signature match RustCircuitPass.repeat. EnvUnowned
// keeps panics inside the native frame and propagates Java callback exceptions.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_dbsp_sqlCompiler_compiler_backend_RustCircuitPass_repeat<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    largest_component: i32,
    requested: i32,
    step: JObject<'local>,
) -> i32 {
    env.with_env(|env| -> jni::errors::Result<i32> {
        let largest = largest_component.max(0) as usize;
        let result = repeat((), largest, usize::try_from(requested).ok(), |(), index| {
            let changed = env.with_local_frame(4, |env| {
                env.call_method(
                    &step,
                    jni_str!("test"),
                    jni_sig!("(I)Z"),
                    &[JValue::Int(index as i32)],
                )?
                .z()
            })?;
            Ok::<_, jni::errors::Error>(((), changed))
        })?;
        Ok(match result {
            RepeatResult::Finished(()) => 0,
            RepeatResult::DidNotConverge { iterations, .. } => iterations as i32,
        })
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
