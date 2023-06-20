
namespace java com.thrift.impl

exception InvalidOperationException {
    1: i32 code,
    2: string description
}

enum LibraryEventType{
    NEW,
    UPDATE
}

struct LibraryEvent {
    1: i32 libraryEventId,
    2: LibraryEventType type,
    3: Book book
}

struct Book {
    1: i32 bookId;
    2: string bookName;
    3: string bookAuthor;
}

service LibraryService {

    LibraryEvent get(1:i32 id) throws (1:InvalidOperationException e),

    void save(1:LibraryEvent resource) throws (1:InvalidOperationException e),

    list <LibraryEvent> getList() throws (1:InvalidOperationException e),

    bool ping() throws (1:InvalidOperationException e)
}