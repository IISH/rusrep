#!/usr/bin/perl

use vars qw/$libpath/;
use FindBin qw($Bin);
BEGIN { $libpath="$Bin" };
use lib "$libpath";
use lib "$libpath/../libs";

use utf8;
use DBI;
my %dbconfig = loadconfig("$Bin/../config/russianrep.config");
use ClioInfra;
use ClioTemplates;
$papersdir = $dbconfig{papersdir};
#print "Content-type: text/html\n\n";
@papers = find_papers($papersdir, $filter);

my ($dbname, $dbhost, $dblogin, $dbpassword) = ($dbconfig{dbname}, $dbconfig{dbhost}, $dbconfig{dblogin}, $dbconfig{dbpassword});
my $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$dbhost",$dblogin,$dbpassword,{AutoCommit=>1,RaiseError=>1,PrintError=>0});

@topics = read_topics($dbh);

my @papers;
foreach $file (@papers)
{
   print "<a href\=\"$uri{$file}\">$file</a><br>\n";
}

sub read_topics
{
    my ($dbh, $DEBUG) = @_;

    $sqlquery = "select topic_id, datatype, topic_name, description, topic_root, topic_name_rus from datasets.topics where 1=1";
    $sqlquery.=" and topic_id in ($topicIDs)" if ($topicIDs);
    $sqlquery.=" and topic_root in ($topicRoots)" if ($topicRoots);
    $sqlquery.=" and datatype in ($datatypes)" if ($datatypes=~/\d+/);
    $sqlquery.=" order by datatype asc";

    my $sth = $dbh->prepare("$sqlquery");
    $sth->execute();

    while (my ($topic_id, $datatype, $topic_name, $description, $topic_root, $topic_name_rus) = $sth->fetchrow_array())
    {
	if ($datatype=~/\d+/)
	{ 
            $udatatype=$datatype;
            $udatatype=~s/\./\_/g;
	    $udatatype="ERRHS_".$udatatype;
	    my @papers = find_papers($papersdir, $udatatype);

	    if ($#papers >= 0)
	    {
		print "$datatype $topic_name\n";
	    }

	    foreach $file (@papers)
	    {
		print "<a href\=\"$uri{$file}\">$file</a><br>\n";
		#print "$paper\n";
	    }
	}
    }
}

sub find_papers
{
   my ($dir, $filter, $DEBUG) = @_;
   my @files;
   opendir(DIR, $dir) or die $!;

   my $uri = $dir;
   $uri=~s/^.+?public_html//g;
   while (my $file = readdir(DIR)) {
        if ($file=~/\.doc/ && (!$filter || $file=~/$filter/i) && $file=~/ERRHS/i)
        {
            push(@files, "$file");
	    $uri{"$file"} = "$uri/$file";
        }
   }
   closedir(DIR);

   return @files;
}
