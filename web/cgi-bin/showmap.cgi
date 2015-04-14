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

my %dbconfig = loadconfig("$Bin/../config/russianrep.config");

my ($dbname, $dbhost, $dblogin, $dbpassword) = ($dbconfig{dbname}, $dbconfig{dbhost}, $dbconfig{dblogin}, $dbconfig{dbpassword});
my $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$dbhost",$dblogin,$dbpassword,{AutoCommit=>1,RaiseError=>1,PrintError=>0});
# Overwrite confuration settings for locale
%dbvars = loaddbconfig($dbh);

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
    'uri=s' => \$uri
    );

open(mapfile, "$Bin/map.template.html");
@map = <mapfile>;
close(mapfile);

%dbvars = loaddbconfig($dbh);
foreach $dbname (keys %dbvars)
{
    $dbconfig{$dbname} = $dbvars{$dbname};
}

$mapintro = $dbconfig{mapintro};
$mapintro_rus = $dbconfig{mapintro_rus};
if ($uri=~/\/ru\//)
{
   $mapintro = $mapintro_rus;
   $lang = 'ru';
}

$workpath = $dbconfig{workpath};
$drupalfiles = $dbconfig{drupal_files};
unless (-e "$workpath/cities_eng.tsv")
{
   $cp = `/bin/cp $Bin/showcase/* $workpath/`;
}

unless ($file)
{
   foreach $line (@map)
   {
       $line=~s/\%\%mapintro\%\%/$mapintro/g;
       $line=~s/\%\%tmpdir\%\%/$drupalfiles/g;
       $line=~s/_eng(\.\w+)/$1/g if ($lang eq 'ru');
       print "$line";
   }
}
