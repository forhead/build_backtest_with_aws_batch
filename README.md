# 基于AWS Batch搭建量化回测系统

## 1.前言

在量化交易策略的开发工作中，需要历史数据来验证交易策略的表现，这种模式称之为回测。历史数据会随着时间的推移产生新的增量，策略研究员也在不断的研究新的交易因子，结合海量的交易品种，这三种条件结合，会有大量的回测任务需要计算，而这种计算任务对于计算资源的需求是突发的，这种突发的计算资源需求，非常适合利用云上的弹性计算资源来实现。本文会介绍一种基于AWS Batch服务建立的量化回测系统，利用这个系统，可以利用容器化技术，快速调度计算资源，完成回测任务。

## 2.方案介绍

方案架构图如下所示，用户会使用Sagemaker Jupyter Notebook进行代码编写和任务提交等操作。用户只需要将策略代码打包成为docker镜像，并推送到托管服务ECR中，然后提交任务到AWS Batch，AWS Batch会自动调度计算资源，把任务分配到计算资源，包含在docker中的程序会根据传入的参数，自动从S3存储桶下载需要的历史数据，计算完成后，上传数据到另一个S3存储桶，并且自动回收计算资源。
![架构图](/images/architecture.png)

## 3.服务介绍

本方案主要使用了AWS Batch作为计算资源的调度服务，要了解方案必须要对AWS Batch的以下几个概念有所了解：

### 3.1AWS Batch
AWS Batch可帮助运行任意规模的批量计算工作负载，该服务根据工作负载的数量和规模自动预配置计算资源并优化工作负载分配，通过使用AWS Batch，不再需要安装或管理批量计算软件，从而使您可以将时间放在分析结果和解决问题。

* compute environment
计算环境是一组用于运行任务的托管或非托管计算资源。使用托管计算环境，您可以在多个详细级别指定所需的计算类型（Fargate 或 EC2）。您可以设置使用特定类型 EC2 实例的计算环境。或者，您可以选择仅指定要使用最新的实例类型。您还可以指定环境的最小、所需和最大 vCPUs 数，以及您愿意为 Spot 实例支付的金额占按需实例价格的百分比以及目标 VPC 子网集。AWS Batch根据需要高效地启动、管理和终止计算类型。

* queue
当您提交AWS Batch作业时，会将其提交到特定的任务队列中，然后作业驻留在那里直到被安排到计算环境中为止。您将一个或多个计算环境与一个作业队列关联。您还可以为这些计算环境甚至跨任务队列本身分配优先级值。

* Job Definition
Job Definition指定作业的运行方式。您可以把任务定义看成是任务中的资源的蓝图。您可以为您的任务提供 IAM 角色，以提供对其他AWS资源的费用。您还可以指定内存和 CPU 要求。任务定义还可以控制容器属性、环境变量和持久性存储的挂载点。任务定义中的许多规范可以通过在提交单个任务时指定新值来覆盖。

* Jobs
提交到 AWS Batch 的工作单位 (如 shell 脚本、Linux 可执行文件或 Docker 容器映像)。会作为一个容器化应用程序运行在AWS Fargate或 Amazon EC2 上，使用您在*Job Definition*中指定的参数。任务可以按名称或按 ID 引用其他任务，并且可以依赖于其他任务的成功完成。

### 3.2 SageMaker Notebook实例
Amazon SageMaker Notebook 实例是一个机器学习 (ML) 计算实例，运行 Jupyter Notebook 应用程序。Jupyter Notebook 提供一种网页形式的简单 IDE，可以在网页中交互式地编写代码和运行代码，直接返回逐段代码的运行结果。同时 Notebook 中还可以穿插必要的说明文档和图片，便于对代码进行说明和解释。

## 4.环境准备

为了实验方便，我们推荐使用Cloudformation快速创建环境。在这个环节中，我们将通过预先准备好的 CloudFormation 模板创建实验的基础网络环境和实验资源。通过以下代码下载CloudFormation文件。
```
git clone https://github.com/forhead/build_backtest_with_aws_batch/blob/main/content/template.yaml
```

通过此链接直接导航到 CloudFormation 控制台创建新堆栈的界面：[CloudFormation](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/create/template)。

* 在控制台选择**Upload a template file**，然后点击**Chose file**，选择上一步中下载的模板
![](/images/4.cloudformation.png)

* 上传完成后点击**Next**
* 在下一步中，列出了模板所需的参数。在 **Stack name** 选项中填入 **backtest**，其他参数保持默认，然后点击**Next**：
* 直接点击**Next**到最终的审核步骤
* 下滑到最下方，勾选**I acknowledge that AWS CloudFormation might create IAM resources with custom names.**，然后点击右下方的**Create stack**按钮：
![](/images/4.cloudformation_acknowledge.png)
* 大概等待5分钟左右，堆栈创建成功

### 5.方案实现
下面跟着我们一步步的搭建基于AWS Batch的量化回测方案。

* 首先执行以下代码，下载backtest.ipynb
```
git clone https://github.com/forhead/build_backtest_with_aws_batch/blob/main/content/backtest.ipynb
```
* 打开[notebook-instances](https://console.aws.amazon.com/sagemaker/home?region=us-east-1#/notebook-instances)的链接，点击上传按钮，上传backtest.ipynb文件
![](/images/5.upload_notebook.png)
