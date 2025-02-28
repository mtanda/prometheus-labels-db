package regexp

// #cgo LDFLAGS: -lsqlite3 -lpcre
// #include <sqlite3.h>
//
// // extension function defined in sqlite3_mod_regexp.c
// extern int sqlite3_extension_init(sqlite3*, char**, const sqlite3_api_routines*);
//
// // Use constructor to register extension function with sqlite.
// void __attribute__((constructor)) init(void) {
//   sqlite3_auto_extension((void*) sqlite3_extension_init);
// }
import "C"
