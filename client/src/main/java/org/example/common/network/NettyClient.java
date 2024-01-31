package org.example.common.network;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.util.CharsetUtil;

import java.util.Timer;
import java.util.TimerTask;

public class NettyClient {

    public static void main(String[] args) throws InterruptedException {
        // 服务器地址和端口
        String host = "127.0.0.1";
        int port = 8080;

        // 创建客户端事件循环组
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            // 创建客户端引导类
            Bootstrap b = new Bootstrap();
            b.group(workerGroup)
                    .channel(NioSocketChannel.class) // 使用 NIO 传输
                    .option(ChannelOption.SO_KEEPALIVE, true) // 设置 TCP 参数
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            // 添加编码器和解码器
                            ch.pipeline().addLast(new StringEncoder(), new StringDecoder());
                            // 添加自定义的业务处理器
                            ch.pipeline().addLast(new NettyClientHandler());
                        }
                    });

            // 连接到服务器
            ChannelFuture channelFuture = b.connect(host, port).sync();
            final Channel channel =   channelFuture.channel();
            loopExecution(channel);

            // 等待客户端通道关闭
            channelFuture.channel().closeFuture().sync();
        } finally {
            // 关闭事件循环组
            workerGroup.shutdownGracefully();
        }
    }

    // 自定义的业务处理器
    public static class NettyClientHandler extends SimpleChannelInboundHandler<String> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String msg) {
            // 处理接收到的消息
            System.out.println("Received message: " + msg);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // 异常处理
            cause.printStackTrace();
            ctx.close();
        }
    }
    public static void loopExecution(final Channel channel){
        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                // 发送消息
                channel.writeAndFlush(Unpooled.copiedBuffer("Hello, Server!", CharsetUtil.UTF_8));
            }
        };
        timer.schedule(task, 0, 1000); // 立即执行，之后每1秒执行一次
    }
}