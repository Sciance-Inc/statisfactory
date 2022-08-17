# Changelog

<!--next-version-placeholder-->

## v0.16.0 (2022-08-17)
### Feature
* Adding support for complete recursive evaluation of +{ statments ([`8f0bb84`](https://github.com/Sciance-Inc/statisfactory/commit/8f0bb849ad36d20f9052122239511ca59ed39cc1))

### Fix
* Dynamic interpolation now support space ([`f06868a`](https://github.com/Sciance-Inc/statisfactory/commit/f06868a82d6aaab8ad52d5b6608030e31a25fee2))

## v0.15.2 (2022-08-17)
### Fix
* Once again trying to solve this fucking port issue ([`beee5c0`](https://github.com/Sciance-Inc/statisfactory/commit/beee5c08a3c99b57047b0347e2fa9ff71c994989))
* Converting port in the post_validation ([`11305d2`](https://github.com/Sciance-Inc/statisfactory/commit/11305d2bcb5780b4dd5f7939a434f5cd7f62ac31))
* Globals/locals None are now properly preserved ([`c979bed`](https://github.com/Sciance-Inc/statisfactory/commit/c979bed2815c6a97ccae1680ab022da387a2a693))
* Removing dead code ([`d4a8aae`](https://github.com/Sciance-Inc/statisfactory/commit/d4a8aaec80dea92003339fc387ed65c9cbf6983d))
* Adding support for nullable ports ([`1785841`](https://github.com/Sciance-Inc/statisfactory/commit/1785841d4c8be255f455c8efb4279152ea2b3f3f))

## v0.15.1 (2022-08-16)
### Fix
* Adding support for None port ([#17](https://github.com/Sciance-Inc/statisfactory/issues/17)) ([`f02a2c6`](https://github.com/Sciance-Inc/statisfactory/commit/f02a2c6e52ddc24e63777a0fb642a7181ed786b3))

## v0.15.0 (2022-08-15)
### Feature
* Adding support for odbc saving ([`b7f5acd`](https://github.com/Sciance-Inc/statisfactory/commit/b7f5acd5e26bf760908093a7a6ac71c8be902b53))

### Fix
* Adding keyword only session ([`7493559`](https://github.com/Sciance-Inc/statisfactory/commit/74935595730ea85d01174d5a4eac330e1e12b49d))

## v0.14.0 (2022-07-20)
### Feature
* Adding support for variadics arguments in craft definition ([`4db0b61`](https://github.com/Sciance-Inc/statisfactory/commit/4db0b61473979869bd79faa1a7f8c744cfa86f80))
* The ODBC interactor connection string is now interpolable ([`35f5506`](https://github.com/Sciance-Inc/statisfactory/commit/35f5506178467b768bd93a23eeabda7d2f653787))

### Fix
* Adding missing manifest builder ([`f97848c`](https://github.com/Sciance-Inc/statisfactory/commit/f97848c9e572fafe18cd6b61f8287e19840113a1))

## v0.13.0 (2022-07-13)
### Feature
* Adding + notations ([`34cc27e`](https://github.com/Sciance-Inc/statisfactory/commit/34cc27eb9838c04574852a30a1c8309ce3e5aa0c))

### Documentation
* Adding compile ([`471bba2`](https://github.com/Sciance-Inc/statisfactory/commit/471bba2d9d9469e7e8e3a40ca13eccc733065a8a))
* Improving documentation ([`ab79d13`](https://github.com/Sciance-Inc/statisfactory/commit/ab79d1317fdf54519719f8cbdac7357580713701))

## v0.12.0 (2022-05-25)
### Feature
* Adding missing licence ([`3a224fe`](https://github.com/Sciance-Inc/statisfactory/commit/3a224fe9092df9ce64b78ef46c244b55df8ca679))

### Fix
* Typo in error path ([`bf44506`](https://github.com/Sciance-Inc/statisfactory/commit/bf44506a54c13e1a1e21d18db3d419f4cbe7e15b))

## v0.11.1 (2022-02-26)
### Fix
* Fixing test ([`f74cb89`](https://github.com/Sciance-Inc/statisfactory/commit/f74cb89f7073684d90837c288f0b6efb99ac8426))

## v0.11.0 (2022-02-26)
### Feature
* Adding introspection commands to the cli ([`30eb01b`](https://github.com/Sciance-Inc/statisfactory/commit/30eb01b65473d3c7bb07f6e0494899f6a29624de))
* Defaulting volatile to default value if provided ([`eee592a`](https://github.com/Sciance-Inc/statisfactory/commit/eee592a1ebe781c5264f9f9f9903490d6bcebe1b))

## v0.10.4 (2022-02-26)
### Fix
* Renaming run parameters to configuration to avoid conflict ([`407e185`](https://github.com/Sciance-Inc/statisfactory/commit/407e185f4cb333be96a42f259bf53a0507f3dcb6))

## v0.10.3 (2022-02-22)
### Fix
* Odbc connector is now initiaed before accessing atributes ([`b7d92ca`](https://github.com/Sciance-Inc/statisfactory/commit/b7d92ca08ab7b3c60350a953e6aac09d1589adf8))

## v0.10.2 (2022-02-22)
### Fix
* Updating odbc query ([`0683b3d`](https://github.com/Sciance-Inc/statisfactory/commit/0683b3d82ba178602359b119630ee53324d762ea))

## v0.10.1 (2022-02-22)
### Fix
* Fixing missing CLI path for import ([`3a2d8ed`](https://github.com/Sciance-Inc/statisfactory/commit/3a2d8ed68956c2aea979b03e45d04dbf318b2192))

## v0.10.0 (2022-02-22)
### Feature
* Adding cli ([`09ba7c2`](https://github.com/Sciance-Inc/statisfactory/commit/09ba7c246f35b1c0d0aa764ecff7701fb84958ed))
* Adding install systemwide ([`213d136`](https://github.com/Sciance-Inc/statisfactory/commit/213d136c6ada9cce4b6871995629634129f90c70))
* Adding run method to the cli ([`dc68b35`](https://github.com/Sciance-Inc/statisfactory/commit/dc68b352a0b1b332af8e0fd71ded9e838f37fc6b))

### Fix
* Bumping version ([`a13854b`](https://github.com/Sciance-Inc/statisfactory/commit/a13854b268947e58215760fff788ece86837685b))

## v0.9.0 (2022-02-20)
### Feature
* Setting trigger on push ([`cac8e56`](https://github.com/Sciance-Inc/statisfactory/commit/cac8e5649fb4168a7232eef5ae75dbb7de5f07a3))

## v0.8.0 (2022-02-20)
### Feature
* Adding custom session ([`4c3e9a3`](https://github.com/Sciance-Inc/statisfactory/commit/4c3e9a3d32b54455000bc66007e31464a08ec2e2))
* Adding custom session injection ([`7bbaccc`](https://github.com/Sciance-Inc/statisfactory/commit/7bbaccca4646a2e3340000d76b655a2fa29963a6))
* Adding validation to the parsed pyproject.toml ([`85f907d`](https://github.com/Sciance-Inc/statisfactory/commit/85f907d25c39ae8be035fa1e62061c113d7c0b27))
* Replaceing pipelines_definitions with parameters ([`d483c84`](https://github.com/Sciance-Inc/statisfactory/commit/d483c84321d7accfb105235882c4e8f970ae6016))
* Adding an error message to the validation ([`f396b00`](https://github.com/Sciance-Inc/statisfactory/commit/f396b00dcfa135dd7f0313ecd8663df3f5a93885))
* Adding support fort custom checks on artifact ([`14154f4`](https://github.com/Sciance-Inc/statisfactory/commit/14154f41a8c04cba699e31d7ecb2858db9343bea))
* Streamlining the artefact creation ([`7bc7102`](https://github.com/Sciance-Inc/statisfactory/commit/7bc71025d3ec741db0fba5f8b5d2b14efeaf0d7f))
* Allowing arbitrary keys in artifacts ([`2e986bc`](https://github.com/Sciance-Inc/statisfactory/commit/2e986bcf1a7d872aa7bdd3538a1193df894d8c73))
* Configurations now supports jinja2 ([`45e0c40`](https://github.com/Sciance-Inc/statisfactory/commit/45e0c403a45512d83d9f83dc3c0d196cfef898c7))

### Fix
* Fixing double trigger in the package release workflow ([`3f3fd9b`](https://github.com/Sciance-Inc/statisfactory/commit/3f3fd9bed720a9f765cd92befff287fb479ba183))
* Removing push from on ([`1fcf470`](https://github.com/Sciance-Inc/statisfactory/commit/1fcf470c0aea5bde00c15513fbea689a0245ac2a))
* Removing marhsmalklow since yaml parsing is now done with pydantic ([`d726a25`](https://github.com/Sciance-Inc/statisfactory/commit/d726a25970d4da7a5d3ce8c80288ed0a34c6b75a))

### Documentation
* Updating catalog exemple ([`421b9b0`](https://github.com/Sciance-Inc/statisfactory/commit/421b9b0290d70e0a3d72faa0cadb12172eee88f8))
* Updating documentation ([`becb728`](https://github.com/Sciance-Inc/statisfactory/commit/becb728875832e0b9fed6c65136a95a55ce32de2))

## v0.7.0 (2022-02-18)


## v0.6.0 (2022-02-18)
### Feature
* Switching to a more flexible artifact handling ([#9](https://github.com/Sciance-Inc/statisfactory/issues/9)) ([`42e6ef7`](https://github.com/Sciance-Inc/statisfactory/commit/42e6ef706cf4aa0ebaaf1642c7be8e854e824c77))

## v0.5.0 (2022-02-18)


## v0.4.0 (2022-02-16)
### Feature
* Statisfactory config is now read from pyproject.toml ([`fa8402a`](https://github.com/Sciance-Inc/statisfactory/commit/fa8402a56d567bc09c3390b3726e5c6d785ff884))

### Fix
* Cleaning olds errors and typo ([`14e31f4`](https://github.com/Sciance-Inc/statisfactory/commit/14e31f4a47880513833236be910b8f1a7bc1104c))
* Ignoring dp cache ([`7573a73`](https://github.com/Sciance-Inc/statisfactory/commit/7573a73b68151cf077c64cdc45f3def7f432b7d6))
* Removing useless code ([`0fae3b5`](https://github.com/Sciance-Inc/statisfactory/commit/0fae3b520ccc8f2a3e3039bc1e63e26b66c32765))
* Adding missing pull_request event ([`303c622`](https://github.com/Sciance-Inc/statisfactory/commit/303c6228365a2d88aea2b1bfec2e1a211bbe97fd))
* Switching to semantic-release ([`fb84139`](https://github.com/Sciance-Inc/statisfactory/commit/fb841392f226d142ce6c7a476ca2e5a63b3d283b))
* Adding missing branches ref ([`0b45b18`](https://github.com/Sciance-Inc/statisfactory/commit/0b45b18d4dfa5a924a621de8c6c02919ec8db727))

## v0.3.1 (2022-02-15)
### Fix
* Lowcasing ([`bc47526`](https://github.com/Sciance-Inc/statisfactory/commit/bc47526449e64456bf3344d452ada0768ce54fab))
