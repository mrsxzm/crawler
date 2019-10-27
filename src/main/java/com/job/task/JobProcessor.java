package com.job.task;

import com.job.dao.JobInfoDao;
import com.job.pojo.JobInfo;
import org.jsoup.Jsoup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.scheduler.BloomFilterDuplicateRemover;
import us.codecraft.webmagic.scheduler.QueueScheduler;
import us.codecraft.webmagic.selector.Html;
import us.codecraft.webmagic.selector.Selectable;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Component
public class JobProcessor implements PageProcessor {
    @Autowired
    private SpringDataPipeline springDataPipeline;
    @Autowired
    private JobInfoDao jobInfoDao;
    private String url = "https://search.51job.com/list/000000,000000,0000,32%252C01,9,99,java,2," +
            "1.html?lang=c&stype=&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99" +
            "&providesalary=99&lonlat=0%2C0&radius=-1&ord_field=0&confirmdate=9&fromType=&dibiaoid=0&address=&line" +
            "=&specialarea=00&from=&welfare=";

    @Override
    public void process(Page page) {
        //在downpage启动，就已经把html下载好
        //通过page得到html,获取所有的节点
        List<Selectable> nodes = page.getHtml().css("div#resultList div.el").nodes();
        //判断是否为空，第一次进来肯定不为空
        if (nodes.size() == 0) {
            //为空，说明得到的html是详情页面，是下面else添加到任务区的
            this.saveInfo(page);

        } else {
            //不为空，将所有的url添加到任务区，
            for (Selectable node : nodes) {
                //获取详情url,直接通过link获取该节点下的url
                String urlInfo = node.links().toString();
                //将url添加到任务区
                page.addTargetRequest(urlInfo);
            }
            //获取下一页的url
            String bkurl = page.getHtml().css("div.p_in li.bk").nodes().get(1).links().toString();
            //将该页面加入任务队列
            page.addTargetRequest(bkurl);
        }

    }

    //进行保存数据
    private void saveInfo(Page page) {
        //new详情对象
        JobInfo jobInfo = new JobInfo();
        //获取html
        Html html = page.getHtml();
        //得到数据，封装到对象
        jobInfo.setTime(new SimpleDateFormat("YYYY:MM:DD").format(new Date()));
        jobInfo.setUrl(page.getUrl().toString());
        //jsoup对得到的html进行解析
        jobInfo.setCompanyAddr(Jsoup.parse(html.css("div.bmsg").nodes().get(1).toString()).text());
        jobInfo.setCompanyInfo(Jsoup.parse(html.css("div.tmsg").toString()).text());
        jobInfo.setCompanyName(html.css("div.cn p.cname a", "text").toString());
        //工作地址和公司地址一样
        jobInfo.setJobAddr(Jsoup.parse(html.css("div.bmsg").nodes().get(1).toString()).text());
        jobInfo.setJobInfo(Jsoup.parse(html.css("div.job_msg").toString()).text());
        jobInfo.setJobName(html.css("div.in div.cn h1", "text").toString());
        //获取薪资
        Integer[] salary = MathSalary.getSalary(html.css("div.cn strong", "text").toString());
        jobInfo.setSalaryMin(salary[0]);
        jobInfo.setSalaryMax(salary[1]);
        //把结果保存起来
        page.putField("jobInfo", jobInfo);
    }

    private Site site = Site.me()
            .setCharset("gbk")//设置编码方式
            .setTimeOut(10 * 1000)//设置超时时间
            .setRetrySleepTime(3000)//设置重试的间隔时间
            .setRetryTimes(3);//设置重试的次数

    @Override
    public Site getSite() {
        return site;
    }

    @Scheduled(initialDelay = 1000, fixedDelay = 100 * 1000)
    public void process() {
        //利用组件启动
        Spider.create(new JobProcessor())
                .addUrl(url)
                .setScheduler(new QueueScheduler().setDuplicateRemover(new BloomFilterDuplicateRemover(100000)))
                .thread(10)
                .addPipeline(this.springDataPipeline)
                .run();
    }













    /* @Override
    public void process(Page page) {
        //解析页面，获取招聘信息详情的url地址
        List<Selectable> list = page.getHtml().css("div#resultList div.el").nodes();


        //判断获取到的集合是否为空，因为是不停的向webmagic的任务组件添加url，可能是详情页 ，也可能列表页
        //第一次，第一页肯定是列表页，进入else,将url添加进去
        if (list.size() == 0) {
            // 如果为空，表示这是招聘详情页,解析页面，获取招聘详情信息，保存数据
            this.saveJobInfo(page);

        } else {
            //如果不为空，表示这是列表页,解析出详情页的url地址，放到任务队列中
            for (Selectable selectable : list) {
                //获取url地址
                String jobInfoUrl = selectable.links().toString();
                //把获取到的url地址放到任务队列中
                page.addTargetRequest(jobInfoUrl);
            }

            //获取下一页的url
            String bkUrl = page.getHtml().css("div.p_in li.bk").nodes().get(1).links().toString();
            //把url放到任务队列中
            page.addTargetRequest(bkUrl);

        }


        String html = page.getHtml().toString();


    }

    //解析页面，获取招聘详情信息，保存数据
    private void saveJobInfo(Page page) {
        //创建招聘详情对象
        JobInfo jobInfo  = new JobInfo();

        //解析页面
        Html html = page.getHtml();

        //获取数据，封装到对象中
        jobInfo.setCompanyName(html.css("div.cn p.cname a","text").toString());
        jobInfo.setCompanyAddr(Jsoup.parse(html.css("div.bmsg").nodes().get(1).toString()).text());
        jobInfo.setCompanyInfo(Jsoup.parse(html.css("div.tmsg").toString()).text());
        jobInfo.setJobName(html.css("div.cn h1","text").toString());
        jobInfo.setJobAddr(html.css("div.cn span.lname","text").toString());
        jobInfo.setJobInfo(Jsoup.parse(html.css("div.job_msg").toString()).text());
        jobInfo.setUrl(page.getUrl().toString());

        //获取薪资
        Integer[] salary = MathSalary.getSalary(html.css("div.cn strong", "text").toString());
        jobInfo.setSalaryMin(salary[0]);
        jobInfo.setSalaryMax(salary[1]);

        //获取发布时间
       // String time = Jsoup.parse(html.css("div.cn p.msg span").nodes().get()).text();
        jobInfo.setTime(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date()));

        //把结果保存起来
        page.putField("jobInfo",jobInfo);
    }


    private Site site = Site.me()
            .setCharset("gbk")//设置编码
            .setTimeOut(10 * 1000)//设置超时时间
            .setRetrySleepTime(3000)//设置重试的间隔时间
            .setRetryTimes(3);//设置重试的次数

    @Override
    public Site getSite() {
        return site;
    }

    @Autowired
    private SpringDataPipeline springDataPipeline;

    //initialDelay当任务启动后，等等多久执行方法
    //fixedDelay每个多久执行方法
    @Scheduled(initialDelay = 1000, fixedDelay = 100 * 1000)
    public void process() {
        Spider.create(new JobProcessor())
                .addUrl(url)
                .setScheduler(new QueueScheduler().setDuplicateRemover(new BloomFilterDuplicateRemover(100000)))
                .thread(10)
                .addPipeline(this.springDataPipeline)
                .run();
    }*/
}
