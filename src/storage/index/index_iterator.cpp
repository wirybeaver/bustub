/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int index, std::optional<ReadPageGuard> read_page_guard,
                                  BufferPoolManager *buffer_pool_manager)
    : page_id_(page_id), index_(index), page_guard_(std::move(read_page_guard)), bpm_(buffer_pool_manager) {
  if (page_id != INVALID_PAGE_ID) {
    assert(page_guard_.has_value());
    page_ = page_guard_.value().template As<LeafPage>();
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() { page_guard_.reset(); };  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  assert(!IsEnd());
  assert(page_ != nullptr);
  return page_->ItemAt(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    return *this;
  }
  if (index_ < page_->GetSize() - 1) {
    index_++;
    return *this;
  }
  auto next_page_id = page_->GetNextPageId();
  if (next_page_id == INVALID_PAGE_ID) {
    page_guard_.reset();
    page_id_ = INVALID_PAGE_ID;
    page_ = nullptr;
    index_ = -1;
    return *this;
  }
  //  auto next_page_guard = bpm_->FetchPageScan(next_page_id);
  auto next_page_guard = bpm_->FetchPageRead(next_page_id);
  page_guard_ = std::move(next_page_guard);
  page_id_ = next_page_id;
  page_ = page_guard_.value().template As<LeafPage>();
  index_ = 0;
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
