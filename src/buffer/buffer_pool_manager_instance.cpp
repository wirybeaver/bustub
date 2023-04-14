//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<frame_id_t>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  auto page = GetAvailableFrameInternal([&]() { return this->AllocatePage(); });
  if (page != nullptr) {
    *page_id = page->page_id_;
  }
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    return &pages_[static_cast<size_t>(frame_id)];
  }
  auto page = GetAvailableFrameInternal([page_id = page_id]() { return page_id; });
  if (page == nullptr) {
    return nullptr;
  }
  disk_manager_->ReadPage(page->page_id_, page->data_);
  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  auto page = &pages_[static_cast<frame_id_t>(frame_id)];
  if (page->pin_count_ <= 0) {
    return false;
  }
  page->pin_count_--;
  if (page->pin_count_ <= 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  page->is_dirty_ = is_dirty;
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  auto page = &pages_[static_cast<frame_id_t>(frame_id)];
  disk_manager_->WritePage(page_id, page->data_);
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t frame_id = 0; frame_id < pool_size_; frame_id++) {
    FlushPgImp(pages_[frame_id].page_id_);
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  auto page = &pages_[static_cast<frame_id_t>(frame_id)];
  if (page->pin_count_ > 0) {
    return false;
  }

  page_table_->Remove(page_id);
  replacer_->Remove(page_id);
  free_list_.emplace_back(page_id);
  page->ResetMemory();
  page->pin_count_ = 0;
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManagerInstance::GetAvailableFrameInternal(const std::function<page_id_t()> &page_id_generator)
    -> Page * {
  frame_id_t frame_id;
  Page *page;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.erase(free_list_.begin());
    page = &pages_[static_cast<size_t>(frame_id)];
  } else {
    auto evict = replacer_->Evict(&frame_id);
    if (!evict) {
      return nullptr;
    }
    page = &pages_[static_cast<size_t>(frame_id)];
    page_table_->Remove(page->page_id_);
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->page_id_, page->GetData());
    }
    page->ResetMemory();
  }
  auto page_id = page_id_generator();
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page_table_->Insert(page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return page;
}

}  // namespace bustub
