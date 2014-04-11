#!/usr/bin/perl

use vars qw/$libpath/;
use FindBin qw($Bin);
BEGIN { $libpath="$Bin" };
use lib "$libpath";
use lib "$libpath/../libs";

use DB_File;
use DBI;
use lib '/home/clio-infra/cgi-bin/libs';
use lib './libs';
use ClioInfra;
use ClioTemplates;
use Ord;
$| = 1;

#$site = "http://node-149.dev.socialhistoryservices.org";
$scriptdir = "/home/clio-infra/cgi-bin";
$countrylink = "datasets/countries";
$indicatorlink ="indicator.html";

$htmltemplate = "$Bin/../templates/countries.tpl";
#@html = loadhtml($htmltemplate);

my %dbconfig = loadconfig("$scriptdir/config/russianrep.config");
$site = $dbconfig{root};
my ($dbname, $dbhost, $dblogin, $dbpassword) = ($dbconfig{dbname}, $dbconfig{dbhost}, $dbconfig{dblogin}, $dbconfig{dbpassword});
my $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$dbhost",$dblogin,$dbpassword,{AutoCommit=>1,RaiseError=>1,PrintError=>0});

my ($dbname, $dbhost, $dblogin, $dbpassword) = ($dbconfig{webdbname}, $dbconfig{dbhost}, $dbconfig{dblogin}, $dbconfig{dbpassword});
my $dbh_web = DBI->connect("dbi:Pg:dbname=$dbname;host=$dbhost",$dblogin,$dbpassword,{AutoCommit=>1,RaiseError=>1,PrintError=>0});

#print "Content-type: text/html\n\n";

#print "Regions\n";
import_class(1);
#import_class(2);

sub import_class
{
   my ($class, $class_prev, $class_name, $DEBUG) = @_;

   $sqlquery = "select distinct histclass$class, datatype from russianrepository where 1=1";
   $sqlquery.=" and histclass$class_prev='$class_name'" if ($class_name);
   $sqlquery.=" order by datatype asc";
   print "SQL $sqlquery\n";

    my $sth = $dbh->prepare("$sqlquery");
    $sth->execute();

    while (my ($histclass, $datatype) = $sth->fetchrow_array())
    {
	if ($datatype=~/^(\d+)/ && $histclass)
	{
	    my $topic = $1;
	    print "$histclass =>$datatype $topic\n";
	    my $index = ordering($region);
	    
	    $next = $class+1;
	    import_class($next, $class, "$histclass") if ($next <= 3); 
	    
#	    $dbh->do("insert into datasets.histclasses (region_name, region_code, active, region_description, region_ord) values ('$region', '$ter_code', 1, '$region', '$index')");
	    $known{$ter_code}++;
        }
   }
};
