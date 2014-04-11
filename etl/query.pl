#!/usr/bin/perl

use vars qw/$libpath/;
use FindBin qw($Bin);
BEGIN { $libpath="$Bin" };
use lib "$libpath";
use lib "$libpath/../lib";

use DBI;
my %dbconfig = loadconfig("$Bin/../config/russianrep.conf");
$dir = $dbconfig{path};
$path = $dbconfig{path};
$database = $dbconfig{maindb};
$dir=~s/\s+$//g;

my ($dbname, $dbhost, $dblogin, $dbpassword) = ($dbconfig{dbname}, $dbconfig{dbhost}, $dbconfig{dblogin}, $dbconfig{dbpassword});
my $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$dbhost",$dblogin,$dbpassword,{AutoCommit=>1,RaiseError=>1,PrintError=>0});

use Getopt::Long;
$DIR = $Bin || "/home/www/clio_infra/clioinfra";

GetOptions (\%h);

my $result = GetOptions(
    \%options,
    'histclass1' => \$histclass1,
    'histclass2' => \$histclass2,
    'histclass3' => \$histclass3,
    'year' => \$year,
    'territory' => \$territory,
    'indicator' => \$indicator,
    'all', 'help',
    'debug=i' => \$DEBUG
);

print "$result\n";
exit(0);
query($dbh, $DEBUG);

sub query
{
    my ($dbh, $DEBUG, %params) = @_;

    $params = "histclass1='мужчины' and histclass2='только в городах' and year='1897'"; # and territory='Акмолинская'";
    $fields = "indicator_id, id, territory, ter_code, town, district, year, month, value,  value_unit, value_label, datatype, histclass1, histclass2, histclass3, histclass4, class1, class2, class3, class4, comment_source, source, volume, page, naborshik_id, comment_naborschik, indicator";

    $sqlquery = "select * from $database where 1=1"; # histclass1='мужчины' and histclass2='только в городах' and year='1897' and territory='Акмолинская'
    $sqlquery.= " and $params" if ($params);

    my $sth = $dbh->prepare("$sqlquery");
    $sth->execute();

    while (my @datarow = $sth->fetchrow_array())
    {
        print "@datarow\n";;
    };
    
};

sub loadconfig
{
    my ($configfile, $DEBUG) = @_;
    my %config;

    open(conf, $configfile);
    while (<conf>)
    {
        my $str = $_;
        $str=~s/\r|\n//g;
        my ($name, $value) = split(/\s*\=\s*/, $str);
        $config{$name} = $value;
    }
    close(conf);

    return %config;
}

