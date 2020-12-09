package com.github.prontera.service;

import com.github.prontera.account.enums.ReservingState;
import com.github.prontera.account.model.response.BalanceReservingResponse;
import com.github.prontera.account.model.response.ConfirmAccountTxnResponse;
import com.github.prontera.account.model.response.QueryAccountResponse;
import com.github.prontera.concurrent.Pools;
import com.github.prontera.domain.Order;
import com.github.prontera.enums.NumericStatusCode;
import com.github.prontera.enums.OrderState;
import com.github.prontera.enums.StatusCode;
import com.github.prontera.exception.ApplicationException;
import com.github.prontera.exception.ResolvableStatusException;
import com.github.prontera.http.client.AccountClient;
import com.github.prontera.http.client.ProductClient;
import com.github.prontera.model.request.CheckoutRequest;
import com.github.prontera.model.request.DiagnoseRequest;
import com.github.prontera.model.response.CheckoutResponse;
import com.github.prontera.model.response.DiagnoseResponse;
import com.github.prontera.model.response.ResolvableResponse;
import com.github.prontera.persistence.OrderMapper;
import com.github.prontera.product.model.response.ConfirmProductTxnResponse;
import com.github.prontera.product.model.response.InventoryReservingResponse;
import com.github.prontera.product.model.response.QueryProductResponse;
import com.github.prontera.util.Responses;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * @author Zhao Junjian
 * @date 2020/01/21
 */
@Service
public class OrderService {

    private static final Logger LOGGER = LogManager.getLogger(OrderService.class);

    private static final int RESERVING_IN_SECS = 5;

    private static final int COMPENSATION_IN_SECS = 3;

    private final OrderMapper orderMapper;

    private final AccountClient accountClient;

    private final ProductClient productClient;

    @Lazy
    @Autowired
    public OrderService(@Nonnull OrderMapper orderMapper,
                        @Nonnull AccountClient accountClient,
                        @Nonnull ProductClient productClient) {
        this.orderMapper = Objects.requireNonNull(orderMapper);
        this.accountClient = accountClient;
        this.productClient = productClient;
    }

    private static <T extends ResolvableResponse> T reassembleIfResolvable(@Nonnull Throwable e, @Nonnull Class<T> returnType) {
        Objects.requireNonNull(e);
        Objects.requireNonNull(returnType);
        ResolvableStatusException rse = null;
        if (e instanceof ResolvableStatusException) {
            rse = (ResolvableStatusException) e;
        } else if (e.getCause() instanceof ResolvableStatusException) {
            rse = (ResolvableStatusException) e.getCause();
        }
        if (rse != null) {
            return Responses.generate(returnType, rse.getStatusCode());
        }
        throw new ApplicationException(e);
    }

    public CompletableFuture<DiagnoseResponse> diagnose(@Nonnull DiagnoseRequest request) {
        Objects.requireNonNull(request);
        final Order order = orderMapper.selectByGuid(request.getGuid());
        if (order == null) {
            throw new ResolvableStatusException(StatusCode.ORDER_NOT_EXISTS);
        }
        final Long orderId = order.getId();
        return accountClient.queryTransaction(orderId)
            .thenCombine(productClient.queryTransaction(orderId), (r1, r2) -> {
                final Map<String, String> map = Maps.newHashMapWithExpectedSize(2);
                map.put("account", ReservingState.parse(r1.getState()).name());
                map.put("product", com.github.prontera.product.enums.ReservingState.parse(r2.getState()).name());
                final DiagnoseResponse response = Responses.generate(DiagnoseResponse.class, StatusCode.OK);
                response.setStateMap(map);
                return response;
            }).exceptionally(e -> reassembleIfResolvable(e, DiagnoseResponse.class));
    }

    public CompletableFuture<CheckoutResponse> checkout(@Nonnull CheckoutRequest request) {
        Objects.requireNonNull(request);
        return doCheckout(request).thenApply(state -> {
            Preconditions.checkArgument(state.isFinalState());
            final CheckoutResponse response;
            if (state == OrderState.CONFIRMED) {
                response = Responses.generate(CheckoutResponse.class, StatusCode.OK);
            } else if (state == OrderState.CANCELLED) {
                response = Responses.generate(CheckoutResponse.class, StatusCode.CANCEL);
            } else if (state == OrderState.CONFLICT) {
                response = Responses.generate(CheckoutResponse.class, StatusCode.CONFLICT);
            } else {
                response = Responses.generate(CheckoutResponse.class, StatusCode.UNKNOWN_RESERVING_STATE);
            }
            return response;
        }).exceptionally(e -> reassembleIfResolvable(e, CheckoutResponse.class));
    }

    public static void main(String[] args) {
        final LocalDateTime now = LocalDateTime.now();
        System.out.println(now);
        //默认失效时间是往后推迟5秒
        System.out.println(now.plusSeconds(RESERVING_IN_SECS));
        System.out.println(now.plusSeconds(RESERVING_IN_SECS).isBefore(now));
    }

    public CompletableFuture<OrderState> doCheckout(@Nonnull CheckoutRequest request) {
        Objects.requireNonNull(request);
        // check if exists the corresponding GUID for idempotency
        final Long guid = request.getGuid();
        final Order order = orderMapper.selectByGuid(guid);
        final CompletableFuture<OrderState> response;
        if (order != null) {
            LOGGER.debug("订单检查，订单已经存在，订单参数为：guid: {} order :{}'", guid, order);
            final OrderState persistedState = order.getState();
            Preconditions.checkArgument(persistedState != OrderState.INVALID);
            if (persistedState.isFinalState()) {
                LOGGER.debug("订单检查，订单已经完成，guid：{}", guid);
                return CompletableFuture.completedFuture(persistedState);
            }
            Preconditions.checkArgument(persistedState == OrderState.PENDING);
            //检查过期时间，以防在5秒内连续提交，就会进入下面的判断。
            if (order.getExpireAt().isBefore(LocalDateTime.now())) {
                //如果失效时间 > 现在时间   =  true
                LOGGER.debug("订单已经超出失效时间，回收和保留订单, guid '{}'", guid);
                response = CompletableFuture.completedFuture(order)
                    .thenCompose(x -> beginTccTransaction(x.getId(), request));
            } else {
                //订单未超出失效时间
                final Long orderId = order.getId();
                // 只需将其设置为超时最终状态并让参与者自动取消即可
                OrderState state = OrderState.CANCELLED;
                //通过CAS比较替换算法进行修改操作，把处理中的订单修改为已超时
                if (orderMapper.compareAndSetState(orderId, OrderState.PENDING, OrderState.CANCELLED) <= 0) {
                    //注意:在生产环境中，你应该强制从主节点进行检索。
                    state = orderMapper.selectByPrimaryKey(orderId).getState(); //此处查询出来state等于2 ，交易超时
                } else {
                    LOGGER.debug("method doCheckout. the order of guid '{}' was cancelled", guid);
                }
                response = CompletableFuture.completedFuture(state);
            }
        } else {
            // 创建新订单
            LOGGER.debug("订单检查，创建新的订单，开始Tcc事务，业务就是在Tcc事务中做的，guid '{}'", guid);
            response = generatePendingOrder(request)
                .thenCompose(x -> beginTccTransaction(x.getId(), request));
        }
        return response;
    }

    CompletableFuture<Order> generatePendingOrder(@Nonnull CheckoutRequest request) {
        final String username = request.getUsername();
        final String productName = request.getProductName();
        final CompletableFuture<QueryAccountResponse> usernameResponse = accountClient.queryAccountByName(username);
        final CompletableFuture<QueryProductResponse> productNameResponse = productClient.queryProductByName(productName);
        return usernameResponse.thenCombineAsync(productNameResponse, (r1, r2) -> {
            final Long userId = r1.getId();
            final Long productId = r2.getId();
            // persist new order
            final Order order = new Order();
            order.setProductId(productId);
            order.setUserId(userId);
            order.setQuantity(request.getQuantity());
            order.setPrice(request.getPrice());
            order.setGuid(request.getGuid());
            final LocalDateTime now = LocalDateTime.now();
            order.setCreateAt(now);
            order.setUpdateAt(now);
            order.setExpireAt(now.plusSeconds(RESERVING_IN_SECS));         //默认失效时间是往后推迟5秒
            order.setState(OrderState.PENDING);         //处理中
            orderMapper.insertSelective(order);
            LOGGER.debug("method generatePendingOrder. persist a new order '{}'", order);
            return order;
        }, Pools.IO);
    }

    //创建好订单后，开始事务去处理业务操作。（采用Tcc事务处理业务）
    CompletableFuture<OrderState> beginTccTransaction(long orderId, CheckoutRequest request) {
        // try reserving
        final String username = request.getUsername();
        final String productName = request.getProductName();
        final Integer price = request.getPrice();
        final Integer quantity = request.getQuantity();
        final int reservingSeconds = RESERVING_IN_SECS + COMPENSATION_IN_SECS;      //事务预留8秒失效时间，也就是说，confirm必须要在8秒内处理完毕
        final CompletableFuture<BalanceReservingResponse> accountReservingResponse =
            accountClient.reservingBalance(username, orderId, price, reservingSeconds); //计算余额到事务表t_account_transaction
        final CompletableFuture<InventoryReservingResponse> productReservingResponse =
            productClient.reservingInventory(productName, orderId, quantity, reservingSeconds);     //扣减库存到事务表t_product_transaction ，
       //到此，try就全部完成了
        LOGGER.debug("到此，try就全部完成了。。。接下来执行confirm");
        //thenCombine的意思是同时执行完accountReservingResponse方法和productReservingResponse的方法后，执行confirmTransaction方法。
        return accountReservingResponse.thenCombine(productReservingResponse, (r1, r2) -> null)
            .thenCompose(x -> confirmTransaction(orderId, username, productName, reservingSeconds));
    }


    //执行confirm
    private CompletableFuture<OrderState> confirmTransaction(long orderId, String username, String productName, int reservingSeconds) {
        //thenCombine方法表示confirmAccountTransaction方法和confirmProductTransaction方法都执行完再继续执行下面的代码。
        return confirmAccountTransaction(orderId, username, reservingSeconds)
            .thenCombine(confirmProductTransaction(orderId, productName, reservingSeconds), (r1, r2) -> {
                OrderState state;
                //如果库存和余额事务都处理成功，state=1
                if (NumericStatusCode.isSuccessful(r1.getCode()) && NumericStatusCode.isSuccessful(r2.getCode())) {
                    state = OrderState.CONFIRMED;
                //如果余额事务和库存事务都超时 ，state=2  交易超时
                } else if (Objects.equals(com.github.prontera.account.enums.StatusCode.TIMEOUT_AND_CANCELLED.code(), r1.getCode()) &&
                    Objects.equals(com.github.prontera.product.enums.StatusCode.TIMEOUT_AND_CANCELLED.code(), r2.getCode())) {
                    state = OrderState.CANCELLED;
                //否则交易冲突 state=3
                } else {
                    state = OrderState.CONFLICT;
                }
                //等所有任务执行完毕，通过CAS算法修改订单状态
                if (orderMapper.compareAndSetState(orderId, OrderState.PENDING, state) <= 0) {
                    // ATTENTION: u should force to retrieve from master node in production environment.
                    state = orderMapper.selectByPrimaryKey(orderId).getState();
                } else {
                    LOGGER.debug("method confirmTransaction. order id '{}' has {}", orderId, state);
                }
                return state;
                // we should define a more specific definition for partial confirm in product environment.
            });
    }

    private CompletableFuture<ConfirmProductTxnResponse> confirmProductTransaction(long orderId, String productName, int reservingSeconds) {
        //延迟8秒+1秒执行产品名为gba的列子去调用http接口。这里用来测试的。
        return CompletableFuture.runAsync(() -> {
            if (Objects.equals("gba", productName)) {
                LockSupport.parkNanos(this, TimeUnit.SECONDS.toNanos(reservingSeconds + 1));
            }
        }, Pools.IO).thenCompose(v -> productClient.confirm(orderId));
    }

    private CompletableFuture<ConfirmAccountTxnResponse> confirmAccountTransaction(long orderId, String username, int reservingSeconds) {
        //延迟8秒+1秒执行用户名为scott的列子去调用http接口。这里用来测试的。
        return CompletableFuture.runAsync(() -> {
            if (Objects.equals("scott", username)) {
                LockSupport.parkNanos(this, TimeUnit.SECONDS.toNanos(reservingSeconds + 1));
            }
        }, Pools.IO).thenCompose(v -> accountClient.confirm(orderId));
    }

}
