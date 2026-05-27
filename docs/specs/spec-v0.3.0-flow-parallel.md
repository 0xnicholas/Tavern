# Tavern V0.3.0: Flow 并行执行

> **状态：DRAFT** — 2026-05-26  
> 前置：spec-v0.3.0-flow-production.md（Phase 3-5 已完成）

## 1. 概述

Phase 1-2 的目标：Flow 支持并发执行多个 `#[start]` 方法，通过 `max_concurrency` 控制并发度。

原型尝试失败的原因：`FlowDispatch::dispatch(&mut self) -> Pin<Box<dyn Future + Send + '_>>` 的 `'_` 生命周期 borrow `&mut self`，导致无法在释放锁后 await Future。

## 2. 核心洞察

**Wrapper 方法模式已在原型中验证有效**。栈上：

```
#[start] async fn step_a(&mut self) → 宏展开为:
  async fn __flow_wrapper_step_a(inner: Arc<Mutex<Self>>) → Result<Value, FlowError> {
      let future = { inner.lock().unwrap().step_a() };  // MutexGuard 在此行末尾 drop
      let result = future.await?;                         // await 不持有 guard
      Ok(serde_json::to_value(result)?)
  }
```

关键：`inner.lock().unwrap().step_a()` 返回的 Future **不 borrow MutexGuard**（因为 Future 由 `step_a()` 自身返回，guard 在 `{}` 作用域末尾 drop）。这是正确的，因为：
- `.step_a()` 是 `async fn(&mut self)` → 返回的 Future 捕获了 `&mut self`
- 但 `&mut self` 来自 `MutexGuard::deref_mut()` — 编译器可能认为 Future borrows guard

实际上：`{ inner.lock().unwrap().step_a() }` 中，`.step_a()` 返回 `impl Future`。由于 `step_a` 签名为 `async fn(&mut self)`，返回的 Future 捕获了 `&mut self`（来自 guard）。编译器推断 Future 的 lifetime 为 guard 的 lifetime — **这就是 `'_` 问题**。

## 3. 解决方案：双重 dispatch

不改变 `FlowDispatch` trait。在 `FlowEngine` 中建立第二套 dispatch 路径：

```
FlowDispatch (pub trait, &mut self, + '_)     ← 对外 API，不变
    ↑
FlowEngine 内部 dispatch (wrapper fn ptr)     ← 新增，并行安全
    ↑
宏生成的 wrapper (Arc<Mutex<Self>>, + Send)   ← 新增
```

### 3.1 宏生成 wrapper

为每个 flow 方法生成一个 `async fn` wrapper（与现有 `__flow_wrapper_*` 不同）：

```rust
// 生成在 impl MacroPipeline 中
async fn __flow_spawn_step_a(inner: Arc<Mutex<Self>>) -> Result<Value, FlowError> {
    // 关键：lock, create future, UNLOCK, then await
    let future = {
        let mut guard = inner.lock().unwrap();
        guard.step_a()  // returns Future that captures &mut Self from guard
    };                  // guard dropped, BUT future still borrows it...
    // PROBLEM: future borrows the dropped guard!
}
```

**修复**：不调用原始 `&mut self` 方法。让 wrapper 直接包含原始方法体：

```rust
// 方案: 宏展开时将方法体 inline 到 wrapper 中
// 或者: 要求用户将状态放在 Arc<Mutex<State>> 中
```

**实际上，最简方案**：不生成 wrapper。在 `FlowEngine` 中用 `tokio::sync::Mutex` + `tokio::spawn` 串行化：

```rust
// FlowEngine 内部
async fn dispatch_parallel_safe(
    inner: &Arc<tokio::sync::Mutex<F>>,
    method: &str,
    input: Value,
) -> Result<Value, FlowError> 
where F: FlowDispatch 
{
    let mut guard = inner.lock().await;
    guard.dispatch(method, input).await  // 持有 guard 贯穿 await
}
```

这串行化了每个方法的 dispatch+await，但允许**多个方法并发等待 semaphore**。在 `tokio::spawn` 中使用：

```rust
let permit = semaphore.acquire().await;
let result = dispatch_parallel_safe(&inner, &name, input).await;
drop(permit);
// dispatch 内部串行（Mutex），但多个 spawn 可并发排队
```

**这是可行的** — `tokio::sync::Mutex` 的 `lock().await` 不会阻塞线程，只挂起 task。多个 task 可并发等待 semaphore 和 mutex。

### 3.2 并行执行流程

```
semaphore.acquire() → Mutex.lock() → dispatch → await Future → Mutex unlock
     ↑                                                                    ↓
  并发排队             串行化（同一时刻仅一个方法调用 dispatch）          释放 semaphore
```

**并发度**：`max_concurrency` 个方法可同时处于 "await Future" 阶段（不持有 Mutex）。但 dispatch 调用本身是串行的（极短，仅 match + Box::pin）。

## 4. 实现

### 4.1 FlowEngine

```rust
pub struct FlowEngine<F> {
    flow: F,
    graph: FlowGraph,
    max_concurrency: usize,
}

impl<F: Flow + FlowDispatch + Send + 'static> FlowEngine<F> {
    pub fn with_max_concurrency(mut self, n: usize) -> Self {
        self.max_concurrency = n.max(1);
        self
    }

    async fn execute_inner_parallel(&mut self) -> Result<Value, FlowError> {
        let inner = Arc::new(tokio::sync::Mutex::new(&mut self.flow));
        // ...but self.flow is already borrowed by &mut self!
    }
}
```

**问题**：`&mut self` 和 `Arc<Mutex<&mut F>>` 冲突。

**修正**：`execute_inner_parallel` 接收 `self`（owned），不是 `&mut self`：

```rust
async fn execute_inner_parallel(self) -> Result<Value, FlowError> {
    let inner = Arc::new(tokio::sync::Mutex::new(self.flow));
    let semaphore = Arc::new(Semaphore::new(self.max_concurrency));
    let mut join_set = JoinSet::new();
    let mut outputs = HashMap::new();

    for name in self.graph.start_nodes() {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let inner = inner.clone();
        let name = name.clone();
        join_set.spawn(async move {
            let _permit = permit;
            let mut guard = inner.lock().await;
            let result = guard.dispatch(&name, Value::Null).await;
            drop(guard);
            (name, result)
        });
    }

    // ...event loop with join_set...
}
```

`start_async` 已经以 `self` (owned) 启动。`execute`（同步）可以内部 clone 或重构。

### 4.2 简化：仅异步路径

`execute()` 保持现有顺序逻辑不变。`start_async()` 切换到 `execute_inner_parallel`：

```rust
pub fn start_async(self) -> (FlowHandle, FlowHandleRef) {
    // ... spawn with execute_inner_parallel ...
}
```

## 5. 关键设计决策

| 决策 | 理由 |
|------|------|
| `tokio::sync::Mutex` 而非 `std::sync::Mutex` | `.await` 期间不阻塞线程 |
| dispatch + await 都在 Mutex 内 | 简化，dispatch 仅 match + Box::pin，极短 |
| 仅异步路径 (`start_async`) 支持并行 | 同步 `execute()` 保持顺序，API 无 breaking change |
| `FlowDispatch` trait 不变 | 零 breaking change |

## 6. 文件变更

```
crates/tavern-flow/src/lib.rs   # +execute_inner_parallel, +max_concurrency, start_async 改用并行路径
```

无宏变更，无 trait 变更。

## 7. 验收

- [ ] `cargo test --workspace` 全绿
- [ ] `max_concurrency=1` 行为与顺序一致
- [ ] `max_concurrency=2` 两个 start 方法并行
- [ ] `FlowDispatch` trait 不变

## 8. 版本历史

| 版本 | 变更 |
|------|------|
| 0.1.0 | 初稿：基于失败经验的精简方案 |
