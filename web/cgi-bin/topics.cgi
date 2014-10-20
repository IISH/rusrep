#!/usr/bin/perl
#
# Copyright (C) 2014 International Institute of Social History.
#
# This program is free software: you can redistribute it and/or  modify
# it under the terms of the GNU Affero General Public License, version 3,
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# As a special exception, the copyright holders give permission to link the
# code of portions of this program with the OpenSSL library under certain
# conditions as described in each individual source file and distribute
# linked combinations including the program with the OpenSSL library. You
# must comply with the GNU Affero General Public License in all respects
# for all of the code used other than as permitted herein. If you modify
# file(s) with this exception, you may extend this exception to your
# version of the file(s), but you are not obligated to do so. If you do not
# wish to do so, delete this exception statement from your version. If you
# delete this exception statement from all source files in the program,
# then also delete it in the license file.

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

$DEBUG = $ARGV[0];
#print "Content-type: text/html\n\n";

#print "Regions\n";

$sqlquery = "select distinct histclass1 from russianrepository order by histclass1 asc";

if ($sqlquery)
{
    my $sth = $dbh->prepare("$sqlquery");
    $sth->execute();

    while (my ($territory) = $sth->fetchrow_array())
    {
	$region = $territory;
	@words = split(/\s+/, $region);
	
#	if ($#words <= 2 && $region)
	$region=~s/^\s+|\s+$//g;
	$region=~s/^\(//g;
	$text = decode_utf8($region);
	if ($text=~/^\s*(\S)(\S)/)
	{
	    $first = $1;
	    $second = lc $2;
	    $lc = lc $first;
	    $up = uc $first;
	    $ordID = ord($lc);
	    $ordIDindex = ord($lc)*100000 + ord($second);
	    my $thisitem = encode_utf8($text);
	    print "$first $text $ordID\n" if ($DEBUG);
	    $data{$text} = $ordID;
	    $dataindex{$text} = $ordIDindex;
	    $datachr{$text} = $up;
	    $index{$up} = $ordID;
	}
    };
};

my $indexSHOW = "<a name=\"index\"></a>&nbsp;";
foreach $up (sort {$index{$a} <=> $index{$b}} keys %index)
{
   $indexSHOW.="<font size=+1><a href=\"#$index{$up}\">$up</a></font>&nbsp;";
} 
print "<p><center>$indexSHOW</center></p>\n";

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
