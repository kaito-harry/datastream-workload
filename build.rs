fn main() {
    // actr gen already generated src/generated/ with prost types and actor code.
    // No build-time code generation needed.
    println!("cargo:rerun-if-changed=src/generated/");
}
