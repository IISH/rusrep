#!/usr/bin/perl

use vars qw/$libpath/;
use FindBin qw($Bin);
BEGIN { $libpath="$Bin" };
use lib "$libpath";
use lib "$libpath/../libs";

use utf8;
use Encode;
use DB_File;
use DBI;
use Configme;
$| = 1;

#$site = "http://node-149.dev.socialhistoryservices.org";
$scriptdir = "/home/clio-infra/cgi-bin";
$countrylink = "datasets/countries";
$indicatorlink ="indicator.html";

$htmltemplate = "$Bin/../templates/countries.tpl";
#@html = loadhtml($htmltemplate);

my %dbconfig = loadconfig("$Bin/../config/russianrep.config");
$site = $dbconfig{root};
my ($dbname, $dbhost, $dblogin, $dbpassword) = ($dbconfig{dbname}, $dbconfig{dbhost}, $dbconfig{dblogin}, $dbconfig{dbpassword});
my $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$dbhost",$dblogin,$dbpassword,{AutoCommit=>1,RaiseError=>1,PrintError=>0});

my ($dbname, $dbhost, $dblogin, $dbpassword) = ($dbconfig{webdbname}, $dbconfig{dbhost}, $dbconfig{dblogin}, $dbconfig{dbpassword});
my $dbh_web = DBI->connect("dbi:Pg:dbname=$dbname;host=$dbhost",$dblogin,$dbpassword,{AutoCommit=>1,RaiseError=>1,PrintError=>0});

use Getopt::Long;

my $result = GetOptions(
    \%options,
    'csvfile=s' => \$csvfile,
    'all', 'help',
    'topic=s' => \$topic,
    'indicator=s' => \$indicator,
    'year=s' => \$year,
    'country=s' => \$country,
    'command=s' => \$command,
    'uri=s' => \$uri,
    'download=s' => \$download,
    'debug=s' => \$debug
);

$DEBUG = $ARGV[0];
#print "Content-type: text/html\n\n";

$topicID = 1;
$histclass_root = 0;
$HTML = 1;
my $DEBUG = 0;

#$filter_datatype = '7.01';
if ($uri=~/^.+\/\S+?\?(\S+)$/)
{
   $command = $1;
   $command=~s/\/\&/ /g;;
   print "CMD $command <br>\n" if ($DEBUG);
   while ($command=~s/(\w+\d+)\=on//)
   {
	my $item = $1;
	if ($item=~/(\d+)/)
	{
	   $topicIDs.= "$1, ";
	}
	$data{$item} = $item;
   }
}
print "$command >> *$topicIDs*<br>" if ($DEBUG);
$topicIDs=~s/\,\s+$//g;
$html = readtopics($topicIDs,'',$filter_datatype);

sub readtopics
{
    my ($topicIDs, $histclass_root, $filter_datatype) = @_;
    my ($html, $datalinks);

    $histclass_root = '0' unless ($histclass_root);
    $sqlquery = "select topic_id, datatype, topic_name, description, topic_root from datasets.topics where 1=1";
    $sqlquery.=" and topic_id in ($topicIDs)" if ($topicIDs=~/\d+/);
    print "$sqlquery\n" if ($DEBUG);

    if ($sqlquery)
    {
        my $sth = $dbh->prepare("$sqlquery");
        $sth->execute();

	while (my ($topic_id, $datatype, $topic_name, $description, $topic_root) = $sth->fetchrow_array())
        {
	    my $topicdata;
	    if (keys %data)
	    {
	        $datalinks.= "<a href=\"/tmp/dataset.$datatype.xls\">Download data in Excel for $topic_name</a><br>";
	        $generator = `/home/clio-infra/cgi-bin/rr/data2excel.py -y 1897 -d $datatype -f dataset.$datatype.xls -D`;
	    }
	    #print "$generator DEBUG <BR>";

	    unless ($topic_root)
	    {
		if ($HTML)
		{
		    my $topic_nameurl = "<a href=\"#\">$topic_name</a>";
		    $htmltopic.="\n<tr><td bgcolor=gray width=\"100%\" colspan=\"4\"><font color=\"#ffffff\">&nbsp;<input type=\"checkbox\" name=\"topic$topic_id\">&nbsp;$topic_nameurl&nbsp;</font></td></tr>\n";
		}
		else
		{
		   print "$topic_name\n" if ($DEBUG);
		}
	    }
	    else
	    {
#	        print "	$datatype $topic_name\n";
		my $topic_nameurl = "<a href=\"#\">$topic_name</a>";
		$htmltopic.="\n<tr><td width=20%>&nbsp;<input type=\"checkbox\" name=\"topic$topic_id\">&nbsp;$datatype $topic_nameurl</td><td>";
	        $topicdata = readclasses($topic_root, $datatype) if (($datatype eq $filter_datatype) || !$filter_datatype);
		$topicdata = "&nbsp;No data\n" unless ($topicdata);
		$htmltopic.="$topicdata</td>"; # if ($topicdate);
	    }

#	    $topicdata = "top";
            #$htmltopic.="<td>$topicdata</td></tr>\n"; # if ($topicdata);
	}
    }

    $downloadlink = "
    $datalinks
    <table width=100% border=0><tr><td>Note: You can click on any historical class if you want to download available data for specific regions of Russia or years.<br>
    By default all data for all regions for selected historical classes will be selected.
    </td><td align=right>
    &nbsp;<input type=\"submit\" value=\"Download Selected Datasets\">
    </td></tr></table>
    ";

    $html.="<table border=1>$htmltopic</table>";
    $html="
    <form name=\"submit\" action=\"/datasets/indicators\" method=\"get\">
    $downloadlink
$html
    $downloadlink
    </form>
    ";
    print "$html\n";
    exit(0);
}

sub readclasses
{
    my ($topicID, $datatype, $histclass_root) = @_;
    my (%classes, $htmlclass);

    $histclass_root = '0' unless ($histclass_root);
    $sqlquery = "select * from datasets.histclasses where datatype='$datatype'"; # and histclass_root='$histclass_root' limit 100;";
    print "$sqlquery\n" if ($DEBUG);

    if ($sqlquery)
    {
        my $sth = $dbh->prepare("$sqlquery");
        $sth->execute();

        # histclass_id | topic | datatype |                                                           histclass1                                                           | histclass2 | histclass3 | histclass_root | level |   index
        while (my ($histclass_id, $topic, $datatype, $histclass1, $histclass2, $histclass3, $histclass_root, $level, $index) = $sth->fetchrow_array())
        {
	    #print "D $histclass1\n";
	    $classes{$histclass1}{$histclass2}{$histclass3} = "$histclass_id%%$topic%%$datatype%%$histclass_root%%$level%%$index";
        };
    };

my $htmllevel1;
foreach $histclass1 (sort keys %classes)
{
    my %class2 = %{$classes{$histclass1}};
    print "\t\t$histclass1\n" if ($histclass1 ne '0' && $DEBUG);
    $histclass1url = "<a href=\"#\">$histclass1</a>";
    $htmllevel1.="\t\t<tr><td width=200 style=\"vertical-align: top;\">&nbsp;<input type=\"checkbox\">&nbsp;$histclass1url</td>\n" if ($histclass1 ne '0' && $HTML);
 
    my $htmllevel2;
    foreach $class2 (sort keys %class2)
    {
	my %class3 = %{$class2{$class2}};
	print "\t\t\t$class2\n" if ($class2 ne '0' && $DEBUG);
	my $class2url = "<a href=\"#\">$class2</a>";
	$htmllevel2.="\t\t\t<tr><td width=200>&nbsp;<input type=\"checkbox\">&nbsp;$class2url\t\t\t</td>\n" if ($class2 ne '0' && $HTML);

	my $htmllevel3;
	foreach $class3 (sort keys %class3)
	{
	    print "\t\t\t\t$class3\n" if ($class3 ne '0' && $DEBUG); 
	    my $class3url = "<a href=\"#\">$class3</a>";
	    $htmllevel3.="\t\t\t\t<tr><td style=\"vertical-align: top;\"><input type=\"checkbox\"> $class3url\t\t\t\t</td></tr>\n" if ($class3 ne '0' && $HTML);
	}
	$htmllevel2.="\n\t\t\t\t<td width=200 style=\"vertical-align: top;\"><table border=0 bgcolor=#efefef>\n&nbsp;$htmllevel3\t\t\t\t</table></td>\n" if ($htmllevel3);
	$htmllevel2.="</tr>" if ($class2 ne '0' && $HTML);
    }
    $htmllevel2 = "\n\t\t\t<td width=600 style=\"vertical-align: top;\"><table border=0 bgcolor=#efefef>\n&nbsp;$htmllevel2\t\t\t</table></td>\n" if ($htmllevel2);
    $htmllevel1.="$htmllevel2</tr>";
}

$htmllevel1 = "\n\t\t<table border=1>\n$htmllevel1\t\t</table>\n";

   return $htmllevel1;
}

foreach $item (sort {$dataindex{$a} <=> $dataindex{$b}} keys %dataindex)
{
    $first = $datachr{$item};
    
    if (!$known{$first})
    {
	print "<p><a href=\"#index\">index</a></p>" if ($published); 
        print "<p><b><a name=\"$data{$item}\">$first</a></b></p>";
	$published++;
    };
    
    my $tab = "&nbsp;&nbsp;&nbsp;";
    print "\t<input type=\"checkbox\">$tab$item<br>\n";
    $known{$first}++;
}
