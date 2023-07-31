//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "fmt/format.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  auto page = GetAvailablePageAndInit([&]() { return this->AllocatePage(); });
  if (page != nullptr) {
    *page_id = page->page_id_;
  }
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    auto fid = iter->second;
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);
    auto page = &pages_[fid];
    page->pin_count_++;
    return page;
  }
  auto page = GetAvailablePageAndInit([page_id = page_id]() { return page_id; }, access_type);
  if (page != nullptr) {
    disk_manager_->ReadPage(page_id, page->GetData());
  }
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  auto fid = iter->second;
  auto page = &pages_[fid];
  page->is_dirty_ = page->is_dirty_ || is_dirty;
  if (page->pin_count_ <= 0) {
    return false;
  }
  if (--page->pin_count_ == 0) {
    replacer_->SetEvictable(fid, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return FlushPageInternal(page_id);
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t frame_id = 0; frame_id < pool_size_; frame_id++) {
    FlushPageInternal(pages_[frame_id].page_id_);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return true;
  }
  auto fid = iter->second;
  auto page = &pages_[fid];
  if (page->pin_count_ > 0) {
    return false;
  }
  replacer_->Remove(fid);
  free_list_.emplace_back(fid);
  page_table_.erase(iter);
  page->ResetMemory();
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->page_id_ = INVALID_PAGE_ID;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto page = FetchPage(page_id);
  if (page == nullptr) {
    throw std::runtime_error("fail to fetch page");
  }
  return {this, FetchPage(page_id)};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  if (page == nullptr) {
    throw std::runtime_error("fail to fetch page");
  }
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  if (page == nullptr) {
    throw std::runtime_error("fail to fetch page");
  }
  page->WLatch();
  return {this, page};
}

// auto BufferPoolManager::FetchPageScan(page_id_t page_id) -> ReadPageGuard {
//   auto page = FetchPage(page_id);
//   if (page == nullptr) {
//     throw std::runtime_error("fail to fetch page");
//   }
//   if (!page->TryRLatch()) {
//     throw std::runtime_error("fail to get read latch on the sibling page");
//   }
//   return {this, page};
// }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

// not thread safe
auto BufferPoolManager::GetAvailablePageAndInit(const std::function<page_id_t()> &page_id_generator,
                                                AccessType access_type) -> Page * {
  frame_id_t fid;
  Page *page;
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.erase(free_list_.begin());
    page = &pages_[fid];
  } else {
    auto evict = replacer_->Evict(&fid);
    if (!evict) {
      return nullptr;
    }
    page = &pages_[fid];
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->page_id_, page->GetData());
    }
    page->is_dirty_ = false;
    page->ResetMemory();
    page_table_.erase(page->page_id_);
  }
  auto page_id = page_id_generator();
  page_table_.emplace(page_id, fid);
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  replacer_->RecordAccess(fid, access_type);
  // recorde access init the lrunode with evictable =false;
  // replacer_->SetEvictable(fid, false);
  return page;
}

auto BufferPoolManager::FlushPageInternal(page_id_t page_id) -> bool {
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto page = &pages_[page_table_[page_id]];
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  return true;
}

}  // namespace bustub
