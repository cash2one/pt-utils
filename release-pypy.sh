#! /bin/bash
# By: pengtao@baidu.com
# Create date:     2014-02-08 

###################################################
# purpose:
#   Release the ubsutils package to hdfs for mapreduce job
#   1. copy remote pypy packge to local
#   2. install new ubsutils.
#   2. back up the remote pypy package
#   3. upload new version
###################################################

package=pypy2.2.1
DATE=`date +%Y%m%d`
YESTODAY=`date +%Y%m%d -d yesterday`

hadoopbin=~/hadoop-client-stoff/hadoop/bin/hadoop
HDFS=/ps/ubs/pengtao/cacheArchive
local_python_dir=./tmp_$package

$hadoopbin fs -test -e $HDFS/$package.tar.gz
if [ "$?" != "0"  ] ; then
    echo "missing package in hdfs $HDFS/$package.tar.gz"
    exit 1
fi


if [ -e $local_python_dir ] ; then
	rm -rf $local_python_dir
fi
mkdir $local_python_dir

echo "downloading $HDFS/$package.tar.gz "
if [ -e $package.tar.gz ] ; then
    rm $package.tar.gz
fi
$hadoopbin fs -copyToLocal $HDFS/$package.tar.gz ./
tar -C $local_python_dir -xzf $package.tar.gz
mv $package.tar.gz $package.bak.tar.gz


echo "installing pypy ubsutils"
$local_python_dir/bin/pypy setup.py install

cd $local_python_dir/
tar -czf $package.new.tar.gz ./bin ./include ./lib ./lib_pypy ./lib-python ./site-packages ./virtualenv_support

echo "uploading new version of $package"
$hadoopbin fs -test -e $HDFS/$package.new.tar.gz
if [ "$?" = "0"  ] ; then
    $hadoopbin fs -rm $HDFS/$package.new.tar.gz
fi
$hadoopbin fs -copyFromLocal $package.new.tar.gz $HDFS/



$hadoopbin fs -test -e $HDFS/$package.${YESTODAY}.tar.gz
if [ "$?" = "0"  ] ; then
    $hadoopbin fs -rm $HDFS/$package.tar.gz
else
    echo " backup the stable old release $HDFS/$package.${YESTODAY}.tar.gz"
    $hadoopbin fs -mv $HDFS/$package.tar.gz $HDFS/$package.${YESTODAY}.tar.gz
fi

$hadoopbin fs -mv $HDFS/$package.new.tar.gz $HDFS/$package.tar.gz

mv $package.new.tar.gz ../






