package org.queue.client.common.network;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.junit.Test;


public class NettyServerTest {

    @Test
    public void tearDown() throws InterruptedException {
        // 服务端端口
        int port = 8080;

        // 创建服务端事件循环组
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            // 创建服务端引导类
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class) // 使用 NIO 传输
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            // 添加编码器和解码器
                            ch.pipeline().addLast(new StringDecoder(), new StringEncoder());
                            // 添加自定义的业务处理器
                            ch.pipeline().addLast(new NettyServerHandler());
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128) // 设置 TCP 参数
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            // 绑定端口并启动服务
            ChannelFuture f = b.bind(port).sync();

            // 等待服务端通道关闭
            f.channel().closeFuture().sync();
        } finally {
            // 关闭事件循环组
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    public static class NettyServerHandler extends SimpleChannelInboundHandler<String> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String msg) {
            // 处理接收到的消息
            System.out.println("Received message: " + msg);
            // 发送响应
            ctx.writeAndFlush("Server received your message: " + msg);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // 异常处理
            cause.printStackTrace();
            ctx.close();
        }
    }
}