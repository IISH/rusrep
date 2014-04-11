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

$sqlquery = "select distinct ter_code, territory from russianrepository order by ter_code desc";

if ($sqlquery)
{
    my $sth = $dbh->prepare("$sqlquery");
    $sth->execute();

    while (my ($ter_code, $territory) = $sth->fetchrow_array())
    {
	$region = $territory;
	@words = split(/\s+/, $region);
	
	if ($#words <= 2 && $region && $ter_code=~/^\d+\_/ && !$known{$ter_code})
	{
	    print "$ter_code;;$region\n";
	    my $index = ordering($region);
	    
	    $dbh->do("insert into datasets.regions (region_name, region_code, active, region_description, region_ord) values ('$region', '$ter_code', 1, '$region', '$index')");
	    $known{$ter_code}++;
	}
    };
};
