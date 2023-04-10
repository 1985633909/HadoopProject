import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author 7200767104 常佳成
 * @since 2023/3/30-22:28
 */
public class HDFS_API_TEST {
    FileSystem fs = null;
    @Before
    public void init()throws Exception{
        //构造配置参数对象
        Configuration conf = new Configuration();
        //设置访问的HDFS 的URI
        conf.set("fs.defaultFS","hdfs://192.168.133.100:9000");
        //设置本机的Hadoop路径
        System.setProperty("hadoop.home.dir","D:\\hadoop");
        //设置客户端访问身份
        System.setProperty("HADOOP_USER_NAME","root");
        //通过FileSystem的静态get()方法获取文件系统客户端对象
        fs = FileSystem.get(conf);
    }
    @Test
    public void AddFileToHdfs()throws IOException{
        //上传文件目的路径
        Path src = new Path("D:/hadoop/hdfs.txt");
        //HDFS的目标路径
        Path dst = new Path("/teshdfs");
        //上传文件方法
        fs.copyFromLocalFile(src,dst);
        //关闭资源
        fs.close();
    }
    //从HDFS中复制文件到本地文件系统
    @Test
    public void DownloadFileToLocal()throws IllegalArgumentException,IOException{
        //下载文件
        fs.copyToLocalFile(new Path("/testFile"),new Path("D:/"));
    }
    //创建，删除，重命名文件
    @Test
    public void MkdirAndDeleteAndRename() throws Exception{
        //创建目录
        fs.mkdirs(new Path("/test1"));
        fs.rename(new Path("/test1"),new Path("/test3"));
        //删除文件夹
        fs.delete(new Path("/test2"),true);

    }
    //查看目录信息
    @Test
    public void ListFiles() throws IOException {
        //获取迭代器对象
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            //打印当前文件名
            System.out.println("fileStatus.getPath().getName() = " + fileStatus.getPath().getName());
            //打印当前文件快大小
            System.out.println("fileStatus.getBlockSize() = " + fileStatus.getBlockSize());
            //打印当前文件权限
            System.out.println(fileStatus.getPermission());
            //打印当前文件内容长度
            System.out.println("fileStatus.getLen() = " + fileStatus.getLen());
            //获取该文件块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation bl : blockLocations) {
                System.out.println("bl-length = " + bl.getLength()+"--block-offset:"+bl.getOffset());
                String[] hosts = bl.getHosts();
                for (String host : hosts) {
                    System.out.println("host = " + host);
                }
            }
        }
    }
    //查看文件及文件夹信息
    @Test
    public void ListFileAll() throws IOException {
        //获取HDFS系统根目录的元数据信息
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        String filelog = "文件夹--    ";
        for (FileStatus fstatus : fileStatuses) {
            //判断是文件还是文件夹
            if (fstatus.isFile()){
                filelog = "文件--     ";
                System.out.println(filelog+fstatus.getPath().getName());
            }
        }
    }
}
